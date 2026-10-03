/*
	This file contains code that manages long-lived merge/split operations using small
	amount of globally-coordinated metadata.
*/

package labelvol

import (
	"fmt"
	"io"
	"sort"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/labels"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

type sizeChange struct {
	oldSize, newSize uint64
}

// Returns the InstanceVersion for the synced labelblk if available or
// defaults to its own instance.
func (d *Data) getMergeIV(v dvid.VersionID) dvid.InstanceVersion {
	syncedLabelblk, err := d.GetSyncedLabelblk()
	if err != nil {
		return dvid.InstanceVersion{d.DataUUID(), v}
	}
	return dvid.InstanceVersion{syncedLabelblk.DataUUID(), v}
}

// MergeLabels handles merging of any number of labels throughout the various label data
// structures.  It assumes that the merges aren't cascading, e.g., there is no attempt
// to merge label 3 into 4 and also 4 into 5.  The caller should have flattened the merges.
//
// The merge is performed synchronously so any failure of the underlying writes is
// returned to the caller.  The merge marker and update flag stay in place on failure,
// leaving the op retryable: the same merge request can be replayed, and an
// interrupted merge is continued from the mutation log when the server restarts.
// ChangeSizeEvent and MergeEndEvent are published only after all RLE writes commit.
//
// EVENTS
//
// labels.MergeStartEvent occurs at very start of merge and transmits labels.DeltaMergeStart struct.
//
// labels.MergeBlockEvent occurs for every block of a merged label and transmits labels.DeltaMerge struct.
//
// labels.MergeEndEvent occurs at end of merge and transmits labels.DeltaMergeEnd struct.
//
func (d *Data) MergeLabels(v dvid.VersionID, m labels.MergeOp) error {
	if len(m.Merged) == 0 {
		return fmt.Errorf("merge requested without any labels to merge")
	}
	dvid.Infof("Merging data %q (labels %s) into label %d ...\n", d.DataName(), m.Merged, m.Target)

	// Serialize merges so retries of a pending merge can't race other merge writes.
	d.mergeMu.Lock()
	defer d.mergeMu.Unlock()

	iv := d.getMergeIV(v)

	// A previous attempt may have failed after marking the labels, leaving the merge
	// marker in place so concurrent reads remain consistent. Detecting the identical
	// in-progress op lets us retry it without double-counting dirty markers.
	resumed, err := labels.MergeActive(iv, m)
	if err != nil {
		return err
	}
	if !resumed {
		// Mark these labels as dirty until done, and make sure we can actually initiate the merge.
		if err := labels.MergeStart(iv, m); err != nil {
			return err
		}
		d.StartUpdate()
	}

	// Signal that we are starting a merge.
	evt := datastore.SyncEvent{d.DataUUID(), labels.MergeStartEvent}
	msg := datastore.SyncMessage{labels.MergeStartEvent, v, labels.DeltaMergeStart{m}}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return fmt.Errorf("can't notify subscribers for event %v: %v", evt, err)
	}

	// Durably record merge intent before any data is changed so a crash can be
	// recovered by continuing this merge from the mutation log on restart.
	if err := labels.LogMerge(d, v, m); err != nil {
		return fmt.Errorf("can't log merge %s for data %q: %v", m, d.DataName(), err)
	}

	// Perform all labelvol writes in a single idempotent commit.
	res, changed, err := d.applyMergeWrites(v, m)
	if err != nil {
		dvid.Criticalf("Merge %s on data %q failed; labels stay marked for retry: %v\n", m, d.DataName(), err)
		return fmt.Errorf("merge %s failed: %v", m, err)
	}

	// Record durable completion of the write phase so recovery can skip this merge.
	if err := labels.LogMutationComplete(d, v, m.MutID); err != nil {
		dvid.Criticalf("can't log merge completion for mutid %d on data %q: %v\n", m.MutID, d.DataName(), err)
	}

	if !changed {
		dvid.Debugf("No changes needed when merging %s into %d.\n", m.Merged, m.Target)
		labels.MergeStop(iv, m)
		d.StopUpdate()
		return nil
	}

	// All writes committed. Publish block-level merge so synced labelblk instances
	// relabel their voxel blocks.
	evt = datastore.SyncEvent{d.DataUUID(), labels.MergeBlockEvent}
	msg = datastore.SyncMessage{labels.MergeBlockEvent, v, labels.DeltaMerge{MergeOp: m, BlockMap: res.blocksChanged}}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		dvid.Criticalf("can't notify subscribers for event %v: %v\n", evt, err)
	}

	// Notify linked labelsz instances of each integrated source label deletion.
	for fromLabel, oldSize := range res.deletedSizes {
		delta := labels.DeltaDeleteSize{
			Label:    fromLabel,
			OldSize:  oldSize,
			OldKnown: true,
		}
		evt := datastore.SyncEvent{d.DataUUID(), labels.ChangeSizeEvent}
		msg := datastore.SyncMessage{labels.ChangeSizeEvent, v, delta}
		if err := datastore.NotifySubscribers(evt, msg); err != nil {
			dvid.Criticalf("can't notify subscribers for event %v: %v\n", evt, err)
		}
	}

	delta := labels.DeltaReplaceSize{
		Label:   m.Target,
		OldSize: res.toLabelSize,
		NewSize: res.toLabelSize + res.addedVoxels,
	}
	evt = datastore.SyncEvent{d.DataUUID(), labels.ChangeSizeEvent}
	msg = datastore.SyncMessage{labels.ChangeSizeEvent, v, delta}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		dvid.Criticalf("can't notify subscribers for event %v: %v\n", evt, err)
	}

	evt = datastore.SyncEvent{d.DataUUID(), labels.MergeEndEvent}
	msg = datastore.SyncMessage{labels.MergeEndEvent, v, labels.DeltaMergeEnd{m}}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		dvid.Criticalf("can't notify subscribers for event %v: %v\n", evt, err)
	}

	// Reached consistent terminal state: clear merge marker and update flag.
	labels.MergeStop(iv, m)
	d.StopUpdate()
	dvid.Infof("Finished merge of %s into label %d on data %q.\n", m.Merged, m.Target, d.DataName())
	return nil
}

// mergeWrites holds the results of applying a merge to labelvol RLE storage,
// including the metadata needed to publish post-commit size events.
type mergeWrites struct {
	blocksChanged map[dvid.IZYXString]struct{}
	toLabelSize   uint64
	addedVoxels   uint64
	deletedSizes  map[uint64]uint64
}

// applyMergeWrites performs the idempotent read/merge/delete/write of labelvol RLEs
// for the given merge op, committing target block puts and source block deletes in a
// single batch. Target blocks are staged before source deletes so a partially applied
// batch can never leave source voxels removed without the target holding them. It is
// safe to re-run after a partially completed attempt or during crash recovery.
func (d *Data) applyMergeWrites(v dvid.VersionID, m labels.MergeOp) (mw mergeWrites, changed bool, err error) {
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return mw, false, fmt.Errorf("data type labelvol had error initializing store: %v", err)
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		return mw, false, fmt.Errorf("data type labelvol requires batch-enabled store, which %q is not", store)
	}

	toLabel := m.Target
	toLabelRLEs, err := d.GetLabelRLEs(v, toLabel)
	if err != nil {
		return mw, false, fmt.Errorf("can't get block-level RLEs for label %d: %v", toLabel, err)
	}
	mw.toLabelSize = toLabelRLEs.NumVoxels()
	mw.blocksChanged = make(map[dvid.IZYXString]struct{})
	mw.deletedSizes = make(map[uint64]uint64)

	// Blocks to remove per integrated source label.
	srcBlocks := make(map[uint64][]dvid.IZYXString, len(m.Merged))

	for fromLabel := range m.Merged {
		dvid.Debugf("Merging label %d to label %d...\n", fromLabel, toLabel)

		fromLabelRLEs, err := d.GetLabelRLEs(v, fromLabel)
		if err != nil {
			return mw, false, fmt.Errorf("can't get block-level RLEs for label %d: %v", fromLabel, err)
		}
		fromLabelSize := fromLabelRLEs.NumVoxels()
		if fromLabelSize == 0 || len(fromLabelRLEs) == 0 {
			dvid.Debugf("Label %d is empty.  Skipping.\n", fromLabel)
			continue
		}
		mw.addedVoxels += fromLabelSize
		mw.deletedSizes[fromLabel] = fromLabelSize

		blocks := make([]dvid.IZYXString, 0, len(fromLabelRLEs))
		for blockStr, fromRLEs := range fromLabelRLEs {
			mw.blocksChanged[blockStr] = struct{}{}
			blocks = append(blocks, blockStr)

			// Integrate the fromLabel RLEs into the toLabel RLEs for this block.
			toRLEs, found := toLabelRLEs[blockStr]
			if found {
				toRLEs.Add(fromRLEs)
			} else {
				toRLEs = fromRLEs
			}
			toLabelRLEs[blockStr] = toRLEs
		}
		srcBlocks[fromLabel] = blocks
	}

	if len(mw.blocksChanged) == 0 {
		return mw, false, nil
	}

	ctx := datastore.NewVersionedCtx(d, v)
	batch := batcher.NewBatch(ctx)

	// Stage the merged target blocks first.
	for blockStr := range mw.blocksChanged {
		serialization, err := toLabelRLEs[blockStr].MarshalBinary()
		if err != nil {
			return mw, true, fmt.Errorf("error serializing RLEs for label %d: %v", toLabel, err)
		}
		batch.Put(NewTKey(toLabel, blockStr), serialization)
	}

	// Stage deletion of all integrated source blocks in the same batch. A label
	// merged into itself keeps its blocks through the puts above.
	for fromLabel, blocks := range srcBlocks {
		if fromLabel == toLabel {
			continue
		}
		for _, blockStr := range blocks {
			batch.Delete(NewTKey(fromLabel, blockStr))
		}
	}

	if err := batch.Commit(); err != nil {
		return mw, true, fmt.Errorf("can't commit merged RLEs into label %d: %v", toLabel, err)
	}
	return mw, true, nil
}

// recoverPendingMerges continues merges found in the mutation log without a
// completion marker, e.g., merges interrupted by a process crash. Merge application
// is idempotent: already-applied merges converge without change. It is invoked at
// instance initialization before sync events are served.
func (d *Data) recoverPendingMerges() {
	rl := d.GetReadLog()
	if rl == nil {
		return
	}
	rootV, err := d.RootVersionID()
	if err != nil {
		dvid.Errorf("Can't get root version for merge recovery of data %q: %v\n", d.DataName(), err)
		return
	}
	versions, err := versionSubtree(rootV)
	if err != nil {
		dvid.Errorf("Can't enumerate versions for merge recovery of data %q: %v\n", d.DataName(), err)
		return
	}
	for _, v := range versions {
		if err := d.recoverVersionMerges(v); err != nil {
			dvid.Criticalf("Merge recovery failed for data %q: %v\n", d.DataName(), err)
		}
	}
}

// versionSubtree returns the root and all descendant versions, parents before
// children, so merge logs are continued in causal order.
func versionSubtree(root dvid.VersionID) ([]dvid.VersionID, error) {
	var all []dvid.VersionID
	stack := []dvid.VersionID{root}
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		all = append(all, cur)
		children, err := datastore.GetChildrenByVersion(cur)
		if err != nil {
			return nil, err
		}
		stack = append(stack, children...)
	}
	return all, nil
}

// recoverVersionMerges continues all incomplete merge ops logged for one version.
func (d *Data) recoverVersionMerges(v dvid.VersionID) error {
	ops, complete, err := labels.ReadMergeLog(d, v)
	if err != nil {
		return err
	}
	uuid, err := datastore.UUIDFromVersion(v)
	if err != nil {
		return err
	}
	for _, op := range ops {
		if _, done := complete[op.MutID]; done {
			continue
		}
		_, changed, err := d.applyMergeWrites(v, op)
		if err != nil {
			return fmt.Errorf("can't continue merge %s (mutid %d) at version %s: %v", op, op.MutID, uuid, err)
		}
		// Mark the op satisfied whether we wrote changes now or found it already
		// converged, preventing later label recreation from being re-merged.
		if lerr := labels.LogMutationComplete(d, v, op.MutID); lerr != nil {
			dvid.Criticalf("can't log recovered merge completion (mutid %d) for data %q: %v\n", op.MutID, d.DataName(), lerr)
		}
		if changed {
			dvid.Infof("Continued interrupted merge %s (mutid %d) for data %q\n", op, op.MutID, d.DataName())
		}
	}
	return nil
}

// SplitLabels splits a portion of a label's voxels into a given split label or, if the given split
// label is 0, a new label, which is returned.  The input is a binary sparse volume and should
// preferably be the smaller portion of a labeled region.  In other words, the caller should chose
// to submit for relabeling the smaller portion of any split.  It is assumed that the given split
// voxels are within the fromLabel set of voxels and will generate unspecified behavior if this is
// not the case.
//
// EVENTS
//
// labels.SplitStartEvent occurs at very start of split and transmits labels.DeltaSplitStart struct.
//
// labels.SplitBlockEvent occurs for every block of a split label and transmits labels.DeltaSplit struct.
//
// labels.SplitEndEvent occurs at end of split and transmits labels.DeltaSplitEnd struct.
//
func (d *Data) SplitLabels(v dvid.VersionID, fromLabel, splitLabel uint64, r io.ReadCloser) (toLabel uint64, err error) {
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		err = fmt.Errorf("Data type labelvol had error initializing store: %v\n", err)
		return
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		err = fmt.Errorf("Data type labelvol requires batch-enabled store, which %q is not\n", store)
		return
	}

	// Create a new label id for this version that will persist to store
	if splitLabel != 0 {
		toLabel = splitLabel
		dvid.Debugf("Splitting subset of label %d into given label %d ...\n", fromLabel, splitLabel)
	} else {
		toLabel, err = d.NewLabel(v)
		if err != nil {
			return
		}
		dvid.Debugf("Splitting subset of label %d into new label %d ...\n", fromLabel, toLabel)
	}

	evt := datastore.SyncEvent{d.DataUUID(), labels.SplitStartEvent}
	splitOpStart := labels.DeltaSplitStart{fromLabel, toLabel}
	splitOpEnd := labels.DeltaSplitEnd{fromLabel, toLabel}

	// Make sure we can split given current merges in progress
	if err := labels.SplitStart(d.getMergeIV(v), splitOpStart); err != nil {
		return toLabel, err
	}
	defer labels.SplitStop(d.getMergeIV(v), splitOpEnd)

	// Signal that we are starting a split.
	msg := datastore.SyncMessage{labels.SplitStartEvent, v, splitOpStart}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return 0, err
	}

	// Read the sparse volume from reader.
	var split dvid.RLEs
	split, err = dvid.ReadRLEs(r)
	if err != nil {
		return
	}
	toLabelSize, _ := split.Stats()

	mutID := d.NewMutationID()
	splitOp := labels.SplitOp{
		MutID:    mutID,
		Target:   fromLabel,
		NewLabel: toLabel,
		RLEs:     split,
	}
	go func() {
		if err := labels.LogSplit(d, v, splitOp); err != nil {
			dvid.Errorf("logging split %q: %v\n", d.DataName(), err)
		}
	}()

	// Partition the split spans into blocks.
	var splitmap dvid.BlockRLEs
	splitmap, err = split.Partition(d.BlockSize)
	if err != nil {
		return
	}

	// Get a sorted list of blocks that cover split.
	splitblks := splitmap.SortedKeys()

	// Publish split event
	deltaSplit := labels.DeltaSplit{
		MutID:        mutID,
		OldLabel:     fromLabel,
		NewLabel:     toLabel,
		Split:        splitmap,
		SortedBlocks: splitblks,
		SplitVoxels:  toLabelSize,
	}

	evt = datastore.SyncEvent{d.DataUUID(), labels.SplitLabelEvent}
	msg = datastore.SyncMessage{labels.SplitLabelEvent, v, deltaSplit}
	if err = datastore.NotifySubscribers(evt, msg); err != nil {
		return
	}

	// Iterate through the split blocks, read the original block.  If the RLEs
	// are identical, just delete the original.  If not, modify the original.
	// TODO: Modifications should be transactional since it's GET-PUT, therefore use
	// hash on block coord to direct it to blockLabel, splitLabel-specific goroutine; we serialize
	// requests to handle concurrency.
	ctx := datastore.NewVersionedCtx(d, v)
	batch := batcher.NewBatch(ctx)

	for _, splitblk := range splitblks {

		// Get original block
		tk := NewTKey(fromLabel, splitblk)
		val, err := store.Get(ctx, tk)
		if err != nil {
			return toLabel, err
		}

		if val == nil {
			return toLabel, fmt.Errorf("Split RLEs at block %s are not part of original label %d", splitblk, fromLabel)
		}
		var rles dvid.RLEs
		if err := rles.UnmarshalBinary(val); err != nil {
			return toLabel, fmt.Errorf("Unable to unmarshal RLE for original labels in block %s", splitblk)
		}

		// Compare and process based on modifications required.
		remain, err := rles.Split(splitmap[splitblk])
		if err != nil {
			return toLabel, err
		}
		if len(remain) == 0 {
			batch.Delete(tk)
		} else {
			rleBytes, err := remain.MarshalBinary()
			if err != nil {
				return toLabel, fmt.Errorf("can't serialize remain RLEs for split of %d: %v\n", fromLabel, err)
			}
			batch.Put(tk, rleBytes)
		}
	}

	if err = batch.Commit(); err != nil {
		err = fmt.Errorf("Batch PUT during split of %q label %d: %v\n", d.DataName(), fromLabel, err)
		return
	}

	// Write the split sparse vol.
	if err = d.writeLabelVol(v, toLabel, splitmap, splitblks); err != nil {
		return
	}

	// Publish change in label sizes.
	delta := labels.DeltaNewSize{
		Label: toLabel,
		Size:  toLabelSize,
	}
	evt = datastore.SyncEvent{d.DataUUID(), labels.ChangeSizeEvent}
	msg = datastore.SyncMessage{labels.ChangeSizeEvent, v, delta}
	if err = datastore.NotifySubscribers(evt, msg); err != nil {
		return
	}

	delta2 := labels.DeltaModSize{
		Label:      fromLabel,
		SizeChange: int64(-toLabelSize),
	}
	evt = datastore.SyncEvent{d.DataUUID(), labels.ChangeSizeEvent}
	msg = datastore.SyncMessage{labels.ChangeSizeEvent, v, delta2}
	if err = datastore.NotifySubscribers(evt, msg); err != nil {
		return
	}

	// Publish split end
	evt = datastore.SyncEvent{d.DataUUID(), labels.SplitEndEvent}
	msg = datastore.SyncMessage{labels.SplitEndEvent, v, splitOpEnd}
	if err = datastore.NotifySubscribers(evt, msg); err != nil {
		return
	}

	return toLabel, nil
}

// SplitCoarseLabels splits a portion of a label's voxels into a given split label or, if the given split
// label is 0, a new label, which is returned.  The input is a binary sparse volume defined by block
// coordinates and should be the smaller portion of a labeled region-to-be-split.
//
// EVENTS
//
// labels.SplitStartEvent occurs at very start of split and transmits labels.DeltaSplitStart struct.
//
// labels.SplitBlockEvent occurs for every block of a split label and transmits labels.DeltaSplit struct.
//
// labels.SplitEndEvent occurs at end of split and transmits labels.DeltaSplitEnd struct.
//
func (d *Data) SplitCoarseLabels(v dvid.VersionID, fromLabel, splitLabel uint64, r io.ReadCloser) (toLabel uint64, err error) {
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		err = fmt.Errorf("Data type labelvol had error initializing store: %v\n", err)
		return
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		err = fmt.Errorf("Data type labelvol requires batch-enabled store, which %q is not\n", store)
		return
	}

	// Create a new label id for this version that will persist to store
	if splitLabel != 0 {
		toLabel = splitLabel
		dvid.Debugf("Splitting coarse subset of label %d into given label %d ...\n", fromLabel, splitLabel)
	} else {
		toLabel, err = d.NewLabel(v)
		if err != nil {
			return
		}
		dvid.Debugf("Splitting coarse subset of label %d into new label %d ...\n", fromLabel, toLabel)
	}

	evt := datastore.SyncEvent{d.DataUUID(), labels.SplitStartEvent}
	splitOpStart := labels.DeltaSplitStart{fromLabel, toLabel}
	splitOpEnd := labels.DeltaSplitEnd{fromLabel, toLabel}

	// Make sure we can split given current merges in progress
	if err := labels.SplitStart(d.getMergeIV(v), splitOpStart); err != nil {
		return toLabel, err
	}
	defer labels.SplitStop(d.getMergeIV(v), splitOpEnd)

	// Signal that we are starting a split.
	msg := datastore.SyncMessage{labels.SplitStartEvent, v, splitOpStart}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return 0, err
	}

	// Read the sparse volume from reader.
	var splits dvid.RLEs
	splits, err = dvid.ReadRLEs(r)
	if err != nil {
		return
	}
	numBlocks, _ := splits.Stats()

	mutID := d.NewMutationID()
	splitOp := labels.SplitOp{
		MutID:    mutID,
		Target:   fromLabel,
		NewLabel: toLabel,
		RLEs:     splits,
		Coarse:   true,
	}
	go func() {
		if err := labels.LogSplit(d, v, splitOp); err != nil {
			dvid.Errorf("logging split %q: %v\n", d.DataName(), err)
		}
	}()

	// Order the split blocks
	splitblks := make(dvid.IZYXSlice, numBlocks)
	n := 0
	for _, rle := range splits {
		p := rle.StartPt()
		run := rle.Length()
		for i := int32(0); i < run; i++ {
			izyx := dvid.IndexZYX{p[0] + i, p[1], p[2]}
			splitblks[n] = izyx.ToIZYXString()
			n++
		}
	}
	sort.Sort(splitblks)

	// Iterate through the split blocks, read the original block and change labels.
	// TODO: Modifications should be transactional since it's GET-PUT, therefore use
	// hash on block coord to direct it to block-specific goroutine; we serialize
	// requests to handle concurrency.
	ctx := datastore.NewVersionedCtx(d, v)
	batch := batcher.NewBatch(ctx)

	var toLabelSize uint64
	for _, splitblk := range splitblks {
		// Get original block
		tk := NewTKey(fromLabel, splitblk)
		val, err := store.Get(ctx, tk)
		if err != nil {
			return toLabel, err
		}
		if val == nil {
			return toLabel, fmt.Errorf("Split block %s is not part of original label %d", splitblk, fromLabel)
		}
		var rles dvid.RLEs
		if err := rles.UnmarshalBinary(val); err != nil {
			return toLabel, fmt.Errorf("Unable to unmarshal RLE for original labels in block %s", splitblk)
		}
		numVoxels, _ := rles.Stats()
		toLabelSize += numVoxels

		// Delete the old block and save the sparse volume but under a new label.
		batch.Delete(tk)
		tk2 := NewTKey(toLabel, splitblk)
		batch.Put(tk2, val)
	}

	if err := batch.Commit(); err != nil {
		dvid.Errorf("Batch PUT during split of %q label %d: %v\n", d.DataName(), fromLabel, err)
	}

	// Publish split event
	deltaSplit := labels.DeltaSplit{
		MutID:        mutID,
		OldLabel:     fromLabel,
		NewLabel:     toLabel,
		SortedBlocks: splitblks,
		SplitVoxels:  toLabelSize,
	}
	evt = datastore.SyncEvent{d.DataUUID(), labels.SplitLabelEvent}
	msg = datastore.SyncMessage{labels.SplitLabelEvent, v, deltaSplit}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return 0, err
	}

	// Publish change in label sizes.
	delta := labels.DeltaNewSize{
		Label: toLabel,
		Size:  toLabelSize,
	}
	evt = datastore.SyncEvent{d.DataUUID(), labels.ChangeSizeEvent}
	msg = datastore.SyncMessage{labels.ChangeSizeEvent, v, delta}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return 0, err
	}

	delta2 := labels.DeltaModSize{
		Label:      fromLabel,
		SizeChange: int64(-toLabelSize),
	}
	evt = datastore.SyncEvent{d.DataUUID(), labels.ChangeSizeEvent}
	msg = datastore.SyncMessage{labels.ChangeSizeEvent, v, delta2}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return 0, err
	}

	// Publish split end
	evt = datastore.SyncEvent{d.DataUUID(), labels.SplitEndEvent}
	msg = datastore.SyncMessage{labels.SplitEndEvent, v, splitOpEnd}
	if err := datastore.NotifySubscribers(evt, msg); err != nil {
		return 0, err
	}
	dvid.Infof("Split %d voxels from label %d to label %d\n", toLabelSize, fromLabel, toLabel)

	return toLabel, nil
}

// write label volume in sorted order if available.
func (d *Data) writeLabelVol(v dvid.VersionID, label uint64, brles dvid.BlockRLEs, sortblks []dvid.IZYXString) error {
	store, err := datastore.GetOrderedKeyValueDB(d)
	if err != nil {
		return fmt.Errorf("Data type labelvol had error initializing store: %v\n", err)
	}
	batcher, ok := store.(storage.KeyValueBatcher)
	if !ok {
		return fmt.Errorf("Data type labelvol requires batch-enabled store, which %q is not\n", store)
	}

	ctx := datastore.NewVersionedCtx(d, v)
	batch := batcher.NewBatch(ctx)
	if sortblks != nil {
		for _, izyxStr := range sortblks {
			serialization, err := brles[izyxStr].MarshalBinary()
			if err != nil {
				return fmt.Errorf("Error serializing RLEs for label %d: %v\n", label, err)
			}
			batch.Put(NewTKey(label, izyxStr), serialization)
		}
	} else {
		for izyxStr, rles := range brles {
			serialization, err := rles.MarshalBinary()
			if err != nil {
				return fmt.Errorf("Error serializing RLEs for label %d: %v\n", label, err)
			}
			batch.Put(NewTKey(label, izyxStr), serialization)
		}
	}
	if err := batch.Commit(); err != nil {
		return fmt.Errorf("Error on updating RLEs for label %d: %v\n", label, err)
	}
	return nil
}
