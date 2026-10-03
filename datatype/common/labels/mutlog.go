/* Handles writing to mutation log for ops on labels. */

package labels

import (
	"encoding/binary"
	"fmt"

	pb "google.golang.org/protobuf/proto"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/datatype/common/proto"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// mergeLogWritable provides a write log without requiring it, allowing data types
// like labelvol that embed the base datastore.Data to durably log merges whenever
// a log store happens to be configured.
type mergeLogWritable interface {
	GetWriteLog() storage.WriteLog
}

type mergeLogReadable interface {
	GetReadLog() storage.ReadLog
}

// LogSplit logs the split of a set of voxels from the underlying label.
func LogSplit(d dvid.Data, v dvid.VersionID, op SplitOp) error {
	uuid, err := datastore.UUIDFromVersion(v)
	if err != nil {
		return err
	}
	logable, ok := d.(storage.LogWritable)
	if !ok {
		return nil // skip logging
	}
	log := logable.GetWriteLog()
	if log == nil {
		return nil
	}
	data, err := serializeSplit(op)
	if err != nil {
		return err
	}
	msg := storage.LogMessage{EntryType: proto.SplitOpType, Data: data}
	return log.Append(d.DataUUID(), uuid, msg)
}

// LogSupervoxelSplit logs the split of a supervoxel into two separate supervoxels.
func LogSupervoxelSplit(d dvid.Data, v dvid.VersionID, op SplitSupervoxelOp) error {
	uuid, err := datastore.UUIDFromVersion(v)
	if err != nil {
		return err
	}
	logable, ok := d.(storage.LogWritable)
	if !ok {
		return nil // skip logging
	}
	log := logable.GetWriteLog()
	if log == nil {
		return nil
	}
	pop := &proto.SupervoxelSplitOp{
		Mutid:       op.MutID,
		Supervoxel:  op.Supervoxel,
		Splitlabel:  op.SplitSupervoxel,
		Remainlabel: op.RemainSupervoxel,
	}
	serialization, err := pb.Marshal(pop)

	if err != nil {
		return err
	}

	msg := storage.LogMessage{EntryType: proto.SupervoxelSplitType, Data: serialization}
	return log.Append(d.DataUUID(), uuid, msg)
}

// LogMerge logs the merge of supervoxels to a label.
func LogMerge(d dvid.Data, v dvid.VersionID, op MergeOp) error {
	uuid, err := datastore.UUIDFromVersion(v)
	if err != nil {
		return err
	}
	logable, ok := d.(mergeLogWritable)
	if !ok {
		return nil // skip logging
	}
	log := logable.GetWriteLog()
	if log == nil {
		return nil
	}
	data, err := serializeMerge(op)
	if err != nil {
		return err
	}
	msg := storage.LogMessage{EntryType: proto.MergeOpType, Data: data}
	return log.Append(d.DataUUID(), uuid, msg)
}

// LogMutationComplete records that all data writes for the mutation with the given
// ID have committed. Paired with the merge op entry, it forms a durable record of
// the merge terminal state, allowing crash recovery to skip finished mutations.
func LogMutationComplete(d dvid.Data, v dvid.VersionID, mutID uint64) error {
	uuid, err := datastore.UUIDFromVersion(v)
	if err != nil {
		return err
	}
	logable, ok := d.(mergeLogWritable)
	if !ok {
		return nil // skip logging
	}
	log := logable.GetWriteLog()
	if log == nil {
		return nil
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], mutID)
	msg := storage.LogMessage{EntryType: proto.MutationCompleteType, Data: buf[:]}
	return log.Append(d.DataUUID(), uuid, msg)
}

// ReadMergeLog reads the mutation log for a version of the given data and returns
// the logged merge ops in append order along with the set of mutation IDs that have
// a completion marker. Merge ops without a completion marker may have been
// interrupted by a crash and should be continued (re-applied idempotently) during
// recovery. A nil log store yields nil slices and no error.
func ReadMergeLog(d dvid.Data, v dvid.VersionID) (ops []MergeOp, complete map[uint64]struct{}, err error) {
	uuid, err := datastore.UUIDFromVersion(v)
	if err != nil {
		return nil, nil, err
	}
	logreadable, ok := d.(mergeLogReadable)
	if !ok {
		return nil, nil, nil
	}
	rl := logreadable.GetReadLog()
	if rl == nil {
		return nil, nil, nil
	}
	msgs, err := rl.ReadAll(d.DataUUID(), uuid)
	if err != nil {
		return nil, nil, err
	}
	complete = make(map[uint64]struct{})
	for _, msg := range msgs {
		switch msg.EntryType {
		case proto.MergeOpType:
			var pop proto.MergeOp
			if err := pb.Unmarshal(msg.Data, &pop); err != nil {
				return nil, nil, fmt.Errorf("unable to unmarshal merge log entry for data %q: %v", d.DataName(), err)
			}
			op := MergeOp{
				MutID:  pop.Mutid,
				Target: pop.Target,
				Merged: make(Set, len(pop.Merged)),
			}
			for _, label := range pop.Merged {
				op.Merged[label] = struct{}{}
			}
			ops = append(ops, op)
		case proto.MutationCompleteType:
			if len(msg.Data) < 8 {
				return nil, nil, fmt.Errorf("malformed mutation completion marker for data %q: got %d bytes, need 8", d.DataName(), len(msg.Data))
			}
			complete[binary.BigEndian.Uint64(msg.Data[:8])] = struct{}{}
		}
	}
	return ops, complete, nil
}

// LogRenumber logs the merge of supervoxels to a label.
func LogRenumber(d dvid.Data, v dvid.VersionID, mutID, origLabel, newLabel uint64) error {
	uuid, err := datastore.UUIDFromVersion(v)
	if err != nil {
		return err
	}
	logable, ok := d.(storage.LogWritable)
	if !ok {
		return nil // skip logging
	}
	log := logable.GetWriteLog()
	if log == nil {
		return nil
	}
	pop := &proto.RenumberOp{
		Mutid:    mutID,
		Target:   origLabel,
		Newlabel: newLabel,
	}
	data, err := pb.Marshal(pop)
	if err != nil {
		return err
	}

	msg := storage.LogMessage{EntryType: proto.RenumberOpType, Data: data}
	return log.Append(d.DataUUID(), uuid, msg)
}

// LogCleave logs the cleave of supervoxels to a label.
func LogCleave(d dvid.Data, v dvid.VersionID, op CleaveOp) error {
	uuid, err := datastore.UUIDFromVersion(v)
	if err != nil {
		return err
	}
	logable, ok := d.(storage.LogWritable)
	if !ok {
		return nil // skip logging
	}
	log := logable.GetWriteLog()
	if log == nil {
		return nil
	}
	data, err := serializeCleave(op)
	if err != nil {
		return err
	}
	msg := storage.LogMessage{EntryType: proto.CleaveOpType, Data: data}
	return log.Append(d.DataUUID(), uuid, msg)
}

// LogMapping logs the mapping of supervoxels to a label.
func LogMapping(d dvid.Data, v dvid.VersionID, op MappingOp) error {
	uuid, err := datastore.UUIDFromVersion(v)
	if err != nil {
		return err
	}
	logable, ok := d.(storage.LogWritable)
	if !ok {
		return nil // skip logging
	}
	log := logable.GetWriteLog()
	if log == nil {
		return nil
	}
	data, err := op.Marshal()
	if err != nil {
		return err
	}
	msg := storage.LogMessage{EntryType: proto.MappingOpType, Data: data}
	return log.Append(d.DataUUID(), uuid, msg)
}

// LogMappings logs a collection of mapping operations to a UUID.
func LogMappings(d dvid.Data, v dvid.VersionID, ops *proto.MappingOps) error {
	uuid, err := datastore.UUIDFromVersion(v)
	if err != nil {
		return err
	}
	logable, ok := d.(storage.LogWritable)
	if !ok {
		return nil // skip logging
	}
	log := logable.GetWriteLog()
	if log == nil {
		return nil
	}
	for _, op := range ops.Mappings {
		data, err := pb.Marshal(op)
		if err != nil {
			return err
		}
		msg := storage.LogMessage{EntryType: proto.MappingOpType, Data: data}
		if err := log.Append(d.DataUUID(), uuid, msg); err != nil {
			return err
		}
	}
	return nil
}

func ReadMappingLog(d dvid.Data, v dvid.VersionID) ([]MappingOp, error) {
	uuid, err := datastore.UUIDFromVersion(v)
	if err != nil {
		return nil, err
	}
	logreadable, ok := d.(storage.LogReadable)
	if !ok {
		return nil, nil
	}
	rl := logreadable.GetReadLog()
	if rl == nil {
		return nil, nil
	}
	msgs, err := rl.ReadAll(d.DataUUID(), uuid)
	if err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, nil
	}
	mappingOps := make([]MappingOp, len(msgs))
	var numMappings int
	for i, msg := range msgs {
		if msg.EntryType != proto.MappingOpType {
			continue
		}

		var op proto.MappingOp
		if err := pb.Unmarshal(msg.Data, &op); err != nil {
			return nil, err
		}
		mappingOps[i].Mapped = op.GetMapped()
		original := op.GetOriginal()
		mappingOps[i].Original = make(Set, len(original))
		for _, label := range original {
			mappingOps[i].Original[label] = struct{}{}
		}
		numMappings++
	}
	mappingOps = mappingOps[:numMappings]
	return mappingOps, nil
}

func StreamLog(d dvid.Data, v dvid.VersionID, ch chan storage.LogMessage) error {
	uuid, err := datastore.UUIDFromVersion(v)
	if err != nil {
		return err
	}
	logreadable, ok := d.(storage.LogReadable)
	if !ok {
		close(ch)
		return nil
	}
	rl := logreadable.GetReadLog()
	if rl == nil {
		close(ch)
		return nil
	}
	return rl.StreamAll(d.DataUUID(), uuid, ch)
}

func LogAffinity(d dvid.Data, v dvid.VersionID, aff Affinity) error {
	uuid, err := datastore.UUIDFromVersion(v)
	if err != nil {
		return err
	}
	logable, ok := d.(storage.LogWritable)
	if !ok {
		return nil // skip logging
	}
	log := logable.GetWriteLog()
	if log == nil {
		return nil
	}
	data, err := serializeAffinity(aff)
	if err != nil {
		return err
	}
	msg := storage.LogMessage{EntryType: proto.AffinityType, Data: data}
	return log.Append(d.DataUUID(), uuid, msg)
}

func serializeSplit(op SplitOp) (serialization []byte, err error) {
	rlesBytes, err := op.RLEs.MarshalBinary()
	if err != nil {
		return nil, err
	}
	svsplits := make(map[uint64]*proto.SVSplit, len(op.SplitMap))
	for supervoxel, split := range op.SplitMap {
		svsplit := new(proto.SVSplit)
		svsplit.Splitlabel = split.Split
		svsplit.Remainlabel = split.Remain
		svsplits[supervoxel] = svsplit
	}
	pop := &proto.SplitOp{
		Mutid:    op.MutID,
		Target:   op.Target,
		Newlabel: op.NewLabel,
		Coarse:   op.Coarse,
		Rles:     rlesBytes,
		Svsplits: svsplits,
	}
	return pb.Marshal(pop)
}

func serializeMerge(op MergeOp) (serialization []byte, err error) {
	merged := make([]uint64, len(op.Merged))
	var i int
	for label := range op.Merged {
		merged[i] = label
		i++
	}
	pop := &proto.MergeOp{
		Mutid:  op.MutID,
		Target: op.Target,
		Merged: merged,
	}
	return pb.Marshal(pop)
}

func serializeCleave(op CleaveOp) (serialization []byte, err error) {
	pop := &proto.CleaveOp{
		Mutid:        op.MutID,
		Target:       op.Target,
		Cleavedlabel: op.CleavedLabel,
		Cleaved:      op.CleavedSupervoxels,
	}
	return pb.Marshal(pop)
}

func serializeAffinity(aff Affinity) (serialization []byte, err error) {
	pop := &proto.Affinity{
		Label1: aff.Label1,
		Label2: aff.Label2,
		Value:  aff.Value,
	}
	return pb.Marshal(pop)
}
