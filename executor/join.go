// Copyright 2016 PingCAP, Inc.
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

// TODO test all hash join testcase with outer.
// TODO explain test HASH_JOIN_HASHER.

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/bitmap"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/stringutil"
)

var (
	_ Executor = &HashJoinExec{}
	_ Executor = &NestedLoopApplyExec{}
)

// HashJoinExec implements the hash join algorithm.
type HashJoinExec struct {
	baseExecutor

	outerExec     Executor
	innerExec     Executor
	innerEstCount float64
	outerFilter   expression.CNFExprs
	outerKeys     []*expression.Column
	innerKeys     []*expression.Column

	// concurrency is the number of partition, build and join workers.
	concurrency    uint
	rowContainer   *hashRowContainer
	hasherFinished chan error
	// joinWorkerWaitGroup is for sync multiple join workers.
	joinWorkerWaitGroup sync.WaitGroup
	finished            atomic.Value
	// closeCh add a lock for closing executor.
	closeCh  chan struct{}
	joinType plannercore.JoinType

	// isHashFromOuter decides which table will globalHashTable be built
	// from. Decision is made by row count, as choosing table with less
	// rows to build hash table will keep lower memory usage. (#6868)
	//
	// TODO: SemiJoin, LeftOuterSemiJoin, AntiSemiJoin and AntiLeftOuterSemiJoin
	// currently do not support building hash table from outer, because:
	//   1. `hasNull` cannot be properly decided when firing onMissMatch()
	//      if outer row is not matched.
	//   2. Joiners of these join types may append rows across result chunks at
	//      least once in tryToMatch(), when inner rows are concurrenly probing
	//      outer hash table.
	isHashFromOuter bool
	// outerBitmap records outer row indexes matched if hash table is
	// built from outer table, to fire onMissMatch() on missed outer rows
	// once altogether.
	outerBitmap *bitmap.ConcurrentBitmap
	// outerBitmapOffsetMap records outer row index offset based on the
	// chunk it belongs to, to calculate the bit index of outerBitmap
	// if hash table is built from outer table.
	outerBitmapOffsetMap map[*chunk.Chunk]int

	requiredRows int64

	// We build individual joiner for each join worker when use chunk-based
	// execution, to avoid the concurrency of joiner.chk and joiner.selected.
	joiners []joiner

	proberChkResourceCh chan *proberChkResource
	proberResultChs     []chan *chunk.Chunk
	joinChkResourceCh   []chan *chunk.Chunk
	joinResultCh        chan *hashjoinWorkerResult

	memTracker  *memory.Tracker // track memory usage.
	prepared    bool
	isOuterJoin bool
}

// proberChkResource stores the result of the join prober fetch worker,
// `dest` is for Chunk reuse: after join workers process the prober chunk which is read from `dest`,
// they'll store the used chunk as `chk`, and then the prober fetch worker will put new data into `chk` and write `chk` into dest.
type proberChkResource struct {
	chk  *chunk.Chunk
	dest chan<- *chunk.Chunk
}

// hashjoinWorkerResult stores the result of join workers,
// `src` is for Chunk reuse: the main goroutine will get the join result chunk `chk`,
// and push `chk` into `src` after processing, join worker goroutines get the empty chunk from `src`
// and push new data into this chunk.
type hashjoinWorkerResult struct {
	chk *chunk.Chunk
	err error
	src chan<- *chunk.Chunk
}

// Close implements the Executor Close interface.
func (e *HashJoinExec) Close() error {
	close(e.closeCh)
	e.finished.Store(true)
	if e.prepared {
		if e.hasherFinished != nil {
			for range e.hasherFinished {
			}
		}
		if e.joinResultCh != nil {
			for range e.joinResultCh {
			}
		}
		if e.proberChkResourceCh != nil {
			close(e.proberChkResourceCh)
			for range e.proberChkResourceCh {
			}
		}
		for i := range e.proberResultChs {
			for range e.proberResultChs[i] {
			}
		}
		for i := range e.joinChkResourceCh {
			close(e.joinChkResourceCh[i])
			for range e.joinChkResourceCh[i] {
			}
		}
		e.proberChkResourceCh = nil
		e.joinChkResourceCh = nil
	}
	e.memTracker = nil

	err := e.baseExecutor.Close()
	return err
}

// Open implements the Executor Open interface.
func (e *HashJoinExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}

	e.prepared = false
	e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaHashJoin)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	e.closeCh = make(chan struct{})
	e.finished.Store(false)
	e.joinWorkerWaitGroup = sync.WaitGroup{}
	return nil
}

// proberExec provides the Executor for probing hash table.
func (e *HashJoinExec) proberExec() Executor {
	if e.isHashFromOuter {
		return e.innerExec
	}
	return e.outerExec
}

// hasherExec provides the Executor for building hash table.
func (e *HashJoinExec) hasherExec() Executor {
	if e.isHashFromOuter {
		return e.outerExec
	}
	return e.innerExec
}

// fetchProberChunks get chunks from fetches chunks from the big table in a background goroutine
// and sends the chunks to multiple channels which will be read by multiple join workers.
func (e *HashJoinExec) fetchProberChunks(ctx context.Context) {
	hasWaitedForHashTable := false
	proberExec := e.proberExec()
	for {
		if e.finished.Load().(bool) {
			return
		}

		var proberResource *proberChkResource
		var ok bool
		select {
		case <-e.closeCh:
			return
		case proberResource, ok = <-e.proberChkResourceCh:
			if !ok {
				return
			}
		}
		proberResult := proberResource.chk
		if e.isOuterJoin {
			required := int(atomic.LoadInt64(&e.requiredRows))
			// TODO outerResult?
			proberResult.SetRequiredRows(required, e.maxChunkSize)
		}
		err := Next(ctx, proberExec, proberResult)
		if err != nil {
			e.joinResultCh <- &hashjoinWorkerResult{
				err: err,
			}
			return
		}
		if !hasWaitedForHashTable {
			// If outer table has no rows, we can simply abort joining.
			if !e.isHashFromOuter && proberResult.NumRows() == 0 {
				e.finished.Store(true)
				return
			}
			jobFinished, hasherErr := e.wait4HashTable()
			if hasherErr != nil {
				e.joinResultCh <- &hashjoinWorkerResult{
					err: hasherErr,
				}
				return
			} else if jobFinished {
				return
			}
			hasWaitedForHashTable = true
		}

		if proberResult.NumRows() == 0 {
			return
		}

		proberResource.dest <- proberResult
	}
}

func (e *HashJoinExec) wait4HashTable() (finished bool, err error) {
	select {
	case <-e.closeCh:
		return true, nil
	case err := <-e.hasherFinished:
		if err != nil {
			return false, err
		}
	}
	if e.rowContainer.Len() == 0 && (e.joinType == plannercore.InnerJoin || e.joinType == plannercore.SemiJoin) {
		return true, nil
	}
	return false, nil
}

var (
	innerResultLabel  fmt.Stringer = stringutil.StringerStr("innerResult")
	hasherResultLabel fmt.Stringer = stringutil.StringerStr("hasherResult")
)

// fetchHasherRows fetches all rows from hasher executor,
// and append them to e.rowContainer.records.
func (e *HashJoinExec) fetchHasherRows(ctx context.Context, chkCh chan<- *chunk.Chunk, doneCh <-chan struct{}) {
	defer close(chkCh)
	hasherExec := e.hasherExec()
	var err error
	for {
		if e.finished.Load().(bool) {
			return
		}
		chk := chunk.NewChunkWithCapacity(hasherExec.base().retFieldTypes, e.ctx.GetSessionVars().MaxChunkSize)
		// TODO WIP seems like it executed twice in TestHashJoin
		err = Next(ctx, hasherExec, chk)
		if err != nil {
			e.hasherFinished <- errors.Trace(err)
			return
		}
		if chk.NumRows() == 0 {
			return
		}
		select {
		case <-doneCh:
			return
		case <-e.closeCh:
			return
		case chkCh <- chk:
		}
	}
}

func (e *HashJoinExec) initializeForProbe() {
	// e.proberResultChs is for transmitting the chunks which store the data of
	// proberExec, it'll be written by probe worker goroutine, and read by join
	// workers.
	e.proberResultChs = make([]chan *chunk.Chunk, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.proberResultChs[i] = make(chan *chunk.Chunk, 1)
	}

	// e.proberChkResourceCh is for transmitting the used proberExec chunks from
	// join workers to proberExec worker.
	e.proberChkResourceCh = make(chan *proberChkResource, e.concurrency)
	proberExec := e.proberExec()
	for i := uint(0); i < e.concurrency; i++ {
		e.proberChkResourceCh <- &proberChkResource{
			chk:  newFirstChunk(proberExec),
			dest: e.proberResultChs[i],
		}
	}

	// e.joinChkResourceCh is for transmitting the reused join result chunks
	// from the main thread to join worker goroutines.
	e.joinChkResourceCh = make([]chan *chunk.Chunk, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.joinChkResourceCh[i] = make(chan *chunk.Chunk, 1)
		e.joinChkResourceCh[i] <- newFirstChunk(e)
	}

	// e.joinResultCh is for transmitting the join result chunks to the main
	// thread.
	e.joinResultCh = make(chan *hashjoinWorkerResult, e.concurrency+1)
}

// fetchProberTableAndProbeHashTable
func (e *HashJoinExec) fetchProberTableAndProbeHashTable(ctx context.Context) {
	e.initializeForProbe()
	e.joinWorkerWaitGroup.Add(1)
	go util.WithRecovery(func() { e.fetchProberChunks(ctx) }, e.handleProberTableFetcherPanic)

	outerKeyColIdx := make([]int, len(e.outerKeys))
	for i := range e.outerKeys {
		outerKeyColIdx[i] = e.outerKeys[i].Index
	}

	// Start e.concurrency join workers to probe hash table and join inner and
	// outer rows.
	for i := uint(0); i < e.concurrency; i++ {
		e.joinWorkerWaitGroup.Add(1)
		workID := i
		go util.WithRecovery(func() { e.runJoinWorker(workID, outerKeyColIdx) }, e.handleJoinWorkerPanic)
	}
	go util.WithRecovery(e.waitJoinWorkersAndCloseResultChan, nil)
}

// handleProberTableFetcherPanic
func (e *HashJoinExec) handleProberTableFetcherPanic(r interface{}) {
	for i := range e.proberResultChs {
		close(e.proberResultChs[i])
	}
	if r != nil {
		e.joinResultCh <- &hashjoinWorkerResult{err: errors.Errorf("%v", r)}
	}
	e.joinWorkerWaitGroup.Done()
}

func (e *HashJoinExec) handleJoinWorkerPanic(r interface{}) {
	if r != nil {
		e.joinResultCh <- &hashjoinWorkerResult{err: errors.Errorf("%v", r)}
	}
	e.joinWorkerWaitGroup.Done()
}

func (e *HashJoinExec) waitJoinWorkersAndCloseResultChan() {
	e.joinWorkerWaitGroup.Wait()
	e.fireOuterBitmapMissMatch()
	close(e.joinResultCh)
}

func (e *HashJoinExec) runJoinWorker(workerID uint, outerKeyColIdx []int) {
	var proberResult *chunk.Chunk
	ok, joinResult := e.getNewJoinResult(workerID)
	if !ok {
		return
	}

	// Read and filter proberResult, and join the proberResult with the hash table rows.
	emptyProberResult := &proberChkResource{
		dest: e.proberResultChs[workerID],
	}
	hCtx := &hashContext{
		allTypes:  retTypes(e.outerExec),
		keyColIdx: outerKeyColIdx,
		h:         fnv.New64(),
		buf:       make([]byte, 1),
	}
	for ok := true; ok; {
		if e.finished.Load().(bool) {
			break
		}
		select {
		case <-e.closeCh:
			return
		case proberResult, ok = <-e.proberResultChs[workerID]:
		}
		if !ok {
			break
		}
		if e.isHashFromOuter {
			ok, joinResult = e.probeOuterHashTable(workerID, proberResult, hCtx, joinResult)
		} else {
			ok, joinResult = e.probeInnerHashTable(workerID, proberResult, hCtx, joinResult)
		}
		if !ok {
			break
		}
		proberResult.Reset()
		emptyProberResult.chk = proberResult
		e.proberChkResourceCh <- emptyProberResult
	}
	if joinResult == nil {
		return
	} else if joinResult.err != nil || (joinResult.chk != nil && joinResult.chk.NumRows() > 0) {
		e.joinResultCh <- joinResult
	}
}

// joinMatchedProberRow2Chunk puts joined matching rows into joinResultCh.
// If hash table is built from inner, onMissMatch will be fired, otherwise
// onMissMatch will be fired altogether while iterating outerBitmap.
func (e *HashJoinExec) joinMatchedProberRow2Chunk(workerID uint, proberRow chunk.Row, hCtx *hashContext,
	joinResult *hashjoinWorkerResult) (bool, *hashjoinWorkerResult) {
	hasherRows, err := e.rowContainer.GetMatchedRows(proberRow, hCtx)
	if err != nil {
		joinResult.err = err
		return false, joinResult
	}
	if len(hasherRows) == 0 { // TODO(fengliyuan): add test case
		if !e.isHashFromOuter {
			e.joiners[workerID].onMissMatch(false, proberRow, joinResult.chk)
		}
		return true, joinResult
	}
	iter := chunk.NewIterator4Slice(hasherRows)
	hasMatch, hasNull := false, false
	for iter.Begin(); iter.Current() != iter.End(); {
		var matched, isNull bool
		if e.isHashFromOuter {
			outerRow := iter.Current()
			innerIter := chunk.NewIterator4Slice([]chunk.Row{proberRow})
			innerIter.Begin()
			matched, _, err = e.joiners[workerID].tryToMatch(outerRow, innerIter, joinResult.chk)
			if matched {
				outerBitmapOffset, err := e.getOuterBitmapOffset(outerRow.Chunk())
				if err != nil {
					joinResult.err = err
					return false, joinResult
				}
				e.outerBitmap.Set(outerBitmapOffset + outerRow.Idx())
			}
			iter.Next()
		} else {
			matched, isNull, err = e.joiners[workerID].tryToMatch(proberRow, iter, joinResult.chk)
		}
		if err != nil {
			joinResult.err = err
			return false, joinResult
		}
		hasMatch = hasMatch || matched
		hasNull = hasNull || isNull

		if joinResult.chk.IsFull() {
			e.joinResultCh <- joinResult
			ok, joinResult := e.getNewJoinResult(workerID)
			if !ok {
				return false, joinResult
			}
		}
	}
	if !e.isHashFromOuter {
		if !hasMatch {
			e.joiners[workerID].onMissMatch(hasNull, proberRow, joinResult.chk)
		}
	}
	return true, joinResult
}

func (e *HashJoinExec) getNewJoinResult(workerID uint) (bool, *hashjoinWorkerResult) {
	joinResult := &hashjoinWorkerResult{
		src: e.joinChkResourceCh[workerID],
	}
	ok := true
	select {
	case <-e.closeCh:
		ok = false
	case joinResult.chk, ok = <-e.joinChkResourceCh[workerID]:
	}
	return ok, joinResult
}

func (e *HashJoinExec) probeInnerHashTable(workerID uint, outerChk *chunk.Chunk, hCtx *hashContext,
	joinResult *hashjoinWorkerResult) (ok bool, _ *hashjoinWorkerResult) {
	var err error
	var selected = make([]bool, 0, chunk.InitialCapacity)
	selected, err = expression.VectorizedFilter(e.ctx, e.outerFilter, chunk.NewIterator4Chunk(outerChk), selected)
	if err != nil {
		joinResult.err = errors.Trace(err)
		return false, joinResult
	}
	for i := range selected {
		if !selected[i] { // process unmatched outer rows
			e.joiners[workerID].onMissMatch(false, outerChk.GetRow(i), joinResult.chk)
		} else { // process matched outer rows
			ok, joinResult = e.joinMatchedProberRow2Chunk(workerID, outerChk.GetRow(i), hCtx, joinResult)
			if !ok {
				return false, joinResult
			}
		}
		if joinResult.chk.IsFull() {
			e.joinResultCh <- joinResult
			ok, joinResult = e.getNewJoinResult(workerID)
			if !ok {
				return false, joinResult
			}
		}
	}
	return true, joinResult
}

func (e *HashJoinExec) probeOuterHashTable(workerID uint, innerChk *chunk.Chunk, hCtx *hashContext,
	joinResult *hashjoinWorkerResult) (ok bool, _ *hashjoinWorkerResult) {
	iterator := chunk.NewIterator4Chunk(innerChk)
	for row := iterator.Begin(); row != iterator.End(); row = iterator.Next() {
		ok, joinResult = e.joinMatchedProberRow2Chunk(workerID, row, hCtx, joinResult)
		if !ok {
			return false, joinResult
		}
		if joinResult.chk.IsFull() {
			e.joinResultCh <- joinResult
			ok, joinResult = e.getNewJoinResult(workerID)
			if !ok {
				return false, joinResult
			}
		}
	}
	return true, joinResult
}

// fireOuterBitmapMissMatch fires onMissMatch() on unmatched outer rows altogether
// based on outerBitmap if hash table is built from outer.
func (e *HashJoinExec) fireOuterBitmapMissMatch() {
	if !e.isHashFromOuter {
		return
	}

	// TODO WIP do not create a new chunk.
	workerID := 0
	joinResult := &hashjoinWorkerResult{
		chk: newFirstChunk(e),
	}
	for chkIdx := 0; chkIdx < e.rowContainer.records.NumChunks(); chkIdx++ {
		chk := e.rowContainer.records.GetChunk(chkIdx)
		offset, err := e.getOuterBitmapOffset(chk)
		if err != nil {
			e.joinResultCh <- &hashjoinWorkerResult{err: err}
			return
		}

		for j, numRows := 0, chk.NumRows(); j < numRows; j++ {
			outerRow := chk.GetRow(j)
			if !e.outerBitmap.UnsafeIsSet(offset + outerRow.Idx()) {
				e.joiners[workerID].onMissMatch(false, outerRow, joinResult.chk)
			}
		}
	}
	if joinResult.chk.NumRows() > 0 {
		e.joinResultCh <- joinResult
	}
}

// Next implements the Executor Next interface.
// hash join constructs the result following these steps:
// step 1. fetch data from inner child and build a hash table;
// step 2. fetch data from outer child in a background goroutine and probe the hash table in multiple join workers.
func (e *HashJoinExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if !e.prepared {
		e.hasherFinished = make(chan error, 1)
		go util.WithRecovery(func() { e.fetchAndBuildHashTable(ctx) }, e.handleFetchAndBuildHashTablePanic)
		e.fetchProberTableAndProbeHashTable(ctx)
		e.prepared = true
	}
	if e.isOuterJoin {
		atomic.StoreInt64(&e.requiredRows, int64(req.RequiredRows()))
	}
	req.Reset()

	result, ok := <-e.joinResultCh
	if !ok {
		return nil
	}
	if result.err != nil {
		e.finished.Store(true)
		return result.err
	}
	req.SwapColumns(result.chk)
	if result.src != nil {
		result.src <- result.chk
	}
	return nil
}

// handleFetchAndBuildHashTablePanic ...
func (e *HashJoinExec) handleFetchAndBuildHashTablePanic(r interface{}) {
	if r != nil {
		e.hasherFinished <- errors.Errorf("%v", r)
	}
	close(e.hasherFinished)
}

func (e *HashJoinExec) fetchAndBuildHashTable(ctx context.Context) {
	// hasherResultCh transfers hasher chunk from fetched to build hash table.
	hasherResultCh := make(chan *chunk.Chunk, 1)
	doneCh := make(chan struct{})
	go util.WithRecovery(func() { e.fetchHasherRows(ctx, hasherResultCh, doneCh) }, nil)

	// TODO: Parallel build hash table. Currently not support because `rowHashMap` is not thread-safe.
	err := e.buildHashTableForList(hasherResultCh)
	if err != nil {
		e.hasherFinished <- errors.Trace(err)
		close(doneCh)
	}
	// Wait fetchInnerRows be finished.
	// 1. if buildHashTableForList fails
	// 2. if outerResult.NumRows() == 0, fetchOutChunks will not wait for inner.
	for range hasherResultCh {
	}

	if e.isHashFromOuter {
		e.buildOuterBitmap()
	}
}

// buildOuterBitmap builds outerBitmap if executor builds hash table from outer.
func (e *HashJoinExec) buildOuterBitmap() {
	e.outerBitmapOffsetMap = make(map[*chunk.Chunk]int, e.rowContainer.records.NumChunks())
	outerLength := 0
	for chkIdx := 0; chkIdx < e.rowContainer.records.NumChunks(); chkIdx++ {
		chk := e.rowContainer.records.GetChunk(chkIdx)
		e.outerBitmapOffsetMap[chk] = outerLength
		outerLength += chk.NumRows()
	}
	e.outerBitmap = bitmap.NewConcurrentBitmap(outerLength)
	e.memTracker.Consume(e.outerBitmap.BytesConsumed())
}

// getOuterBitmapOffset gets outerBitmap offset of an outer row.
func (e *HashJoinExec) getOuterBitmapOffset(chunk *chunk.Chunk) (outerBitmapOffset int, err error) {
	outerBitmapOffset, ok := e.outerBitmapOffsetMap[chunk]
	// This should never happen.
	if !ok {
		err = errors.Errorf(
			"get outer row offset error, chunk: %v, outerBitmapOffsetMap: %v",
			chunk,
			e.outerBitmapOffsetMap,
		)
		return
	}
	return
}

// buildHashTableForList builds hash table from `list`.
func (e *HashJoinExec) buildHashTableForList(hasherResultCh <-chan *chunk.Chunk) error {
	innerKeyColIdx := make([]int, len(e.innerKeys))
	for i := range e.innerKeys {
		innerKeyColIdx[i] = e.innerKeys[i].Index
	}
	allTypes := e.innerExec.base().retFieldTypes
	hCtx := &hashContext{
		allTypes:  allTypes,
		keyColIdx: innerKeyColIdx,
		h:         fnv.New64(),
		buf:       make([]byte, 1),
	}
	initList := chunk.NewList(allTypes, e.initCap, e.maxChunkSize)
	e.rowContainer = newHashRowContainer(e.ctx, int(e.innerEstCount), hCtx, initList)
	e.rowContainer.GetMemTracker().AttachTo(e.memTracker)
	e.rowContainer.GetMemTracker().SetLabel(innerResultLabel)
	var filter expression.CNFExprs
	if e.isHashFromOuter {
		filter = e.outerFilter
	}
	for chk := range hasherResultCh {
		if e.finished.Load().(bool) {
			return nil
		}
		err := e.rowContainer.PutChunk(e.ctx, chk, filter)
		if err != nil {
			return err
		}
	}
	return nil
}

// NestedLoopApplyExec is the executor for apply.
type NestedLoopApplyExec struct {
	baseExecutor

	innerRows   []chunk.Row
	cursor      int
	innerExec   Executor
	outerExec   Executor
	innerFilter expression.CNFExprs
	outerFilter expression.CNFExprs
	outer       bool

	joiner joiner

	outerSchema []*expression.CorrelatedColumn

	outerChunk       *chunk.Chunk
	outerChunkCursor int
	outerSelected    []bool
	innerList        *chunk.List
	innerChunk       *chunk.Chunk
	innerSelected    []bool
	innerIter        chunk.Iterator
	outerRow         *chunk.Row
	hasMatch         bool
	hasNull          bool

	memTracker *memory.Tracker // track memory usage.
}

// Close implements the Executor interface.
func (e *NestedLoopApplyExec) Close() error {
	e.innerRows = nil

	e.memTracker = nil
	return e.outerExec.Close()
}

var innerListLabel fmt.Stringer = stringutil.StringerStr("innerList")

// Open implements the Executor interface.
func (e *NestedLoopApplyExec) Open(ctx context.Context) error {
	err := e.outerExec.Open(ctx)
	if err != nil {
		return err
	}
	e.cursor = 0
	e.innerRows = e.innerRows[:0]
	e.outerChunk = newFirstChunk(e.outerExec)
	e.innerChunk = newFirstChunk(e.innerExec)
	e.innerList = chunk.NewList(retTypes(e.innerExec), e.initCap, e.maxChunkSize)

	e.memTracker = memory.NewTracker(e.id, e.ctx.GetSessionVars().MemQuotaNestedLoopApply)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	e.innerList.GetMemTracker().SetLabel(innerListLabel)
	e.innerList.GetMemTracker().AttachTo(e.memTracker)

	return nil
}

func (e *NestedLoopApplyExec) fetchSelectedOuterRow(ctx context.Context, chk *chunk.Chunk) (*chunk.Row, error) {
	outerIter := chunk.NewIterator4Chunk(e.outerChunk)
	for {
		if e.outerChunkCursor >= e.outerChunk.NumRows() {
			err := Next(ctx, e.outerExec, e.outerChunk)
			if err != nil {
				return nil, err
			}
			if e.outerChunk.NumRows() == 0 {
				return nil, nil
			}
			e.outerSelected, err = expression.VectorizedFilter(e.ctx, e.outerFilter, outerIter, e.outerSelected)
			if err != nil {
				return nil, err
			}
			e.outerChunkCursor = 0
		}
		outerRow := e.outerChunk.GetRow(e.outerChunkCursor)
		selected := e.outerSelected[e.outerChunkCursor]
		e.outerChunkCursor++
		if selected {
			return &outerRow, nil
		} else if e.outer {
			e.joiner.onMissMatch(false, outerRow, chk)
			if chk.IsFull() {
				return nil, nil
			}
		}
	}
}

// fetchAllInners reads all data from the inner table and stores them in a List.
func (e *NestedLoopApplyExec) fetchAllInners(ctx context.Context) error {
	err := e.innerExec.Open(ctx)
	defer terror.Call(e.innerExec.Close)
	if err != nil {
		return err
	}
	e.innerList.Reset()
	innerIter := chunk.NewIterator4Chunk(e.innerChunk)
	for {
		err := Next(ctx, e.innerExec, e.innerChunk)
		if err != nil {
			return err
		}
		if e.innerChunk.NumRows() == 0 {
			return nil
		}

		e.innerSelected, err = expression.VectorizedFilter(e.ctx, e.innerFilter, innerIter, e.innerSelected)
		if err != nil {
			return err
		}
		for row := innerIter.Begin(); row != innerIter.End(); row = innerIter.Next() {
			if e.innerSelected[row.Idx()] {
				e.innerList.AppendRow(row)
			}
		}
	}
}

// Next implements the Executor interface.
func (e *NestedLoopApplyExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	req.Reset()
	for {
		if e.innerIter == nil || e.innerIter.Current() == e.innerIter.End() {
			if e.outerRow != nil && !e.hasMatch {
				e.joiner.onMissMatch(e.hasNull, *e.outerRow, req)
			}
			e.outerRow, err = e.fetchSelectedOuterRow(ctx, req)
			if e.outerRow == nil || err != nil {
				return err
			}
			e.hasMatch = false
			e.hasNull = false

			for _, col := range e.outerSchema {
				*col.Data = e.outerRow.GetDatum(col.Index, col.RetType)
			}
			err = e.fetchAllInners(ctx)
			if err != nil {
				return err
			}
			e.innerIter = chunk.NewIterator4List(e.innerList)
			e.innerIter.Begin()
		}

		matched, isNull, err := e.joiner.tryToMatch(*e.outerRow, e.innerIter, req)
		e.hasMatch = e.hasMatch || matched
		e.hasNull = e.hasNull || isNull

		if err != nil || req.IsFull() {
			return err
		}
	}
}
