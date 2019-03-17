// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"fmt"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/planner"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// Compiler compiles an ast.StmtNode to a physical plan.
type Compiler struct {
	Ctx sessionctx.Context
}

// Compile compiles an ast.StmtNode to a physical plan.
func (c *Compiler) Compile(ctx context.Context, stmtNode ast.StmtNode) (*ExecStmt, error) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("executor.Compile", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}

	infoSchema := GetInfoSchema(c.Ctx)
	if err := plannercore.Preprocess(c.Ctx, stmtNode, infoSchema); err != nil {
		return nil, errors.Trace(err)
	}

	finalPlan, err := planner.Optimize(c.Ctx, stmtNode, infoSchema)
	if err != nil {
		return nil, errors.Trace(err)
	}

	CountStmtNode(stmtNode, c.Ctx.GetSessionVars().InRestrictedSQL)
	isExpensive := logExpensiveQuery(stmtNode, finalPlan)

	return &ExecStmt{
		InfoSchema: infoSchema,
		Plan:       finalPlan,
		Expensive:  isExpensive,
		Cacheable:  plannercore.Cacheable(stmtNode),
		Text:       stmtNode.Text(),
		StmtNode:   stmtNode,
		Ctx:        c.Ctx,
	}, nil
}

func logExpensiveQuery(stmtNode ast.StmtNode, finalPlan plannercore.Plan) (expensive bool) {
	expensive = isExpensiveQuery(finalPlan)
	if !expensive {
		return
	}

	const logSQLLen = 1024
	sql := stmtNode.Text()
	if len(sql) > logSQLLen {
		sql = fmt.Sprintf("%s len(%d)", sql[:logSQLLen], len(sql))
	}
	logutil.Logger(context.Background()).Warn("EXPENSIVE_QUERY", zap.String("SQL", sql))
	return
}

func isExpensiveQuery(p plannercore.Plan) bool {
	switch x := p.(type) {
	case plannercore.PhysicalPlan:
		return isPhysicalPlanExpensive(x)
	case *plannercore.Execute:
		return isExpensiveQuery(x.Plan)
	case *plannercore.Insert:
		if x.SelectPlan != nil {
			return isPhysicalPlanExpensive(x.SelectPlan)
		}
	case *plannercore.Delete:
		if x.SelectPlan != nil {
			return isPhysicalPlanExpensive(x.SelectPlan)
		}
	case *plannercore.Update:
		if x.SelectPlan != nil {
			return isPhysicalPlanExpensive(x.SelectPlan)
		}
	}
	return false
}

func isPhysicalPlanExpensive(p plannercore.PhysicalPlan) bool {
	expensiveRowThreshold := int64(config.GetGlobalConfig().Log.ExpensiveThreshold)
	if int64(p.StatsCount()) > expensiveRowThreshold {
		return true
	}

	for _, child := range p.Children() {
		if isPhysicalPlanExpensive(child) {
			return true
		}
	}

	return false
}

// CountStmtNode records the number of statements with the same type.
func CountStmtNode(stmtNode ast.StmtNode, inRestrictedSQL bool) {
	if inRestrictedSQL {
		return
	}

	typeLabel := GetStmtLabel(stmtNode)
	metrics.StmtNodeCounter.WithLabelValues(typeLabel).Inc()

	if !config.GetGlobalConfig().Status.RecordQPSbyDB {
		return
	}

	dbLabels := getStmtDbLabel(stmtNode)
	for dbLabel := range dbLabels {
		metrics.DbStmtNodeCounter.WithLabelValues(dbLabel, typeLabel).Inc()
	}
}

func getStmtDbLabel(stmtNode ast.StmtNode) map[string]struct{} {
	dbLabelSet := make(map[string]struct{})

	switch x := stmtNode.(type) {
	case *ast.AlterTableStmt:
		dbLabel := x.Table.Schema.O
		dbLabelSet[dbLabel] = struct{}{}
	case *ast.CreateIndexStmt:
		dbLabel := x.Table.Schema.O
		dbLabelSet[dbLabel] = struct{}{}
	case *ast.CreateTableStmt:
		dbLabel := x.Table.Schema.O
		dbLabelSet[dbLabel] = struct{}{}
	case *ast.InsertStmt:
		dbLabels := getDbFromResultNode(x.Table.TableRefs)
		for _, db := range dbLabels {
			dbLabelSet[db] = struct{}{}
		}
		dbLabels = getDbFromResultNode(x.Select)
		for _, db := range dbLabels {
			dbLabelSet[db] = struct{}{}
		}
	case *ast.DropIndexStmt:
		dbLabel := x.Table.Schema.O
		dbLabelSet[dbLabel] = struct{}{}
	case *ast.DropTableStmt:
		tables := x.Tables
		for _, table := range tables {
			dbLabel := table.Schema.O
			if _, ok := dbLabelSet[dbLabel]; !ok {
				dbLabelSet[dbLabel] = struct{}{}
			}
		}
	case *ast.SelectStmt:
		dbLabels := getDbFromResultNode(x)
		for _, db := range dbLabels {
			dbLabelSet[db] = struct{}{}
		}
	case *ast.UpdateStmt:
		if x.TableRefs != nil {
			dbLabels := getDbFromResultNode(x.TableRefs.TableRefs)
			for _, db := range dbLabels {
				dbLabelSet[db] = struct{}{}
			}
		}
	case *ast.DeleteStmt:
		if x.TableRefs != nil {
			dbLabels := getDbFromResultNode(x.TableRefs.TableRefs)
			for _, db := range dbLabels {
				dbLabelSet[db] = struct{}{}
			}
		}
	}

	return dbLabelSet
}

func getDbFromResultNode(resultNode ast.ResultSetNode) []string { //may have duplicate db name
	var dbLabels []string

	if resultNode == nil {
		return dbLabels
	}

	switch x := resultNode.(type) {
	case *ast.TableSource:
		return getDbFromResultNode(x.Source)
	case *ast.SelectStmt:
		if x.From != nil {
			return getDbFromResultNode(x.From.TableRefs)
		}
	case *ast.TableName:
		dbLabels = append(dbLabels, x.DBInfo.Name.O)
	case *ast.Join:
		if x.Left != nil {
			dbs := getDbFromResultNode(x.Left)
			if dbs != nil {
				for _, db := range dbs {
					dbLabels = append(dbLabels, db)
				}
			}
		}

		if x.Right != nil {
			dbs := getDbFromResultNode(x.Right)
			if dbs != nil {
				for _, db := range dbs {
					dbLabels = append(dbLabels, db)
				}
			}
		}
	}

	return dbLabels
}

// GetStmtLabel generates a label for a statement.
func GetStmtLabel(stmtNode ast.StmtNode) string {
	switch x := stmtNode.(type) {
	case *ast.AlterTableStmt:
		return "AlterTable"
	case *ast.AnalyzeTableStmt:
		return "AnalyzeTable"
	case *ast.BeginStmt:
		return "Begin"
	case *ast.CommitStmt:
		return "Commit"
	case *ast.CreateDatabaseStmt:
		return "CreateDatabase"
	case *ast.CreateIndexStmt:
		return "CreateIndex"
	case *ast.CreateTableStmt:
		return "CreateTable"
	case *ast.CreateViewStmt:
		return "CreateView"
	case *ast.CreateUserStmt:
		return "CreateUser"
	case *ast.DeleteStmt:
		return "Delete"
	case *ast.DropDatabaseStmt:
		return "DropDatabase"
	case *ast.DropIndexStmt:
		return "DropIndex"
	case *ast.DropTableStmt:
		return "DropTable"
	case *ast.ExplainStmt:
		return "Explain"
	case *ast.InsertStmt:
		if x.IsReplace {
			return "Replace"
		}
		return "Insert"
	case *ast.LoadDataStmt:
		return "LoadData"
	case *ast.RollbackStmt:
		return "RollBack"
	case *ast.SelectStmt:
		return "Select"
	case *ast.SetStmt, *ast.SetPwdStmt:
		return "Set"
	case *ast.ShowStmt:
		return "Show"
	case *ast.TruncateTableStmt:
		return "TruncateTable"
	case *ast.UpdateStmt:
		return "Update"
	case *ast.GrantStmt:
		return "Grant"
	case *ast.RevokeStmt:
		return "Revoke"
	case *ast.DeallocateStmt:
		return "Deallocate"
	case *ast.ExecuteStmt:
		return "Execute"
	case *ast.PrepareStmt:
		return "Prepare"
	case *ast.UseStmt:
		return "Use"
	}
	return "other"
}

// GetInfoSchema gets TxnCtx InfoSchema if snapshot schema is not set,
// Otherwise, snapshot schema is returned.
func GetInfoSchema(ctx sessionctx.Context) infoschema.InfoSchema {
	sessVar := ctx.GetSessionVars()
	var is infoschema.InfoSchema
	if snap := sessVar.SnapshotInfoschema; snap != nil {
		is = snap.(infoschema.InfoSchema)
		logutil.Logger(context.Background()).Info("use snapshot schema", zap.Uint64("conn", sessVar.ConnectionID), zap.Int64("schemaVersion", is.SchemaMetaVersion()))
	} else {
		is = sessVar.TxnCtx.InfoSchema.(infoschema.InfoSchema)
	}
	return is
}
