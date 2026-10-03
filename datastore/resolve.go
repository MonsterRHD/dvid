/*
	This file implements the durable, recoverable operation behind the repo
	"resolve" endpoint.  A single resolve request is frozen into a resolveOpT
	(recorded within its repo's gob metadata) and advanced through fixed
	phases: deletion of conflicts per data instance, commit of extension
	nodes, final assigned merge, and completion.  Every stage is persisted
	before the next begins, so retries -- including retries after process
	restart -- resume unfinished work without duplicate nodes or deletions.
*/

package datastore

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// Phases of a resolve operation, persisted in the operation record.
type resolvePhase uint8

const (
	phaseDeleting resolvePhase = iota // scanning instances and writing tombstones
	phaseCommitting                   // locking extension nodes
	phaseMerging                      // creating the final merge child
	phaseDone                         // final child persisted and usable
	phaseStale                        // terminal: frozen plan no longer matches repo state
)

// resolveOpT is the frozen, persistently recoverable state of one resolve.
type resolveOpT struct {
	id   dvid.UUID // unique operation id, assigned at creation

	parents []dvid.UUID         // ordered parent UUIDs, frozen (priority order)
	data    []dvid.InstanceName // target data instances, frozen
	note    string              // note for final merge; first submission wins

	created time.Time
	updated time.Time

	phase resolvePhase

	// extensions[i] is the extension node UUID for parents[i], once created,
	// or dvid.NilUUID.  At most one extension per parent.
	extensions []dvid.UUID

	// extCommitted[i] tracks whether extensions[i] has been locked.
	extCommitted []bool

	// instDone records which target instances have finished all deletions.
	instDone map[dvid.InstanceName]bool

	// child is the pre-generated, then actual, final merge child UUID.
	child dvid.UUID

	// Frozen fingerprint used to reject stale plans on resume.
	parentChildren [][]dvid.VersionID // children versions of each parent at freeze
	instID         []dvid.InstanceID  // InstanceID of each target at freeze
	instDataUUID   []dvid.UUID        // DataUUID of each target at freeze

	errMsg string // last operation error, for diagnostics

	// mu guards phase and child against concurrent readers (waiting duplicate
	// submissions) and gob encoding.  Not persisted.
	mu sync.RWMutex
}

func newResolveOp(id dvid.UUID, parents []dvid.UUID, data []dvid.InstanceName, note string) *resolveOpT {
	t := time.Now().Round(0)
	return &resolveOpT{
		id:           id,
		parents:      parents,
		data:         data,
		note:         note,
		created:      t,
		updated:      t,
		phase:        phaseDeleting,
		extensions:   make([]dvid.UUID, len(parents)),
		extCommitted: make([]bool, len(parents)),
		instDone:     make(map[dvid.InstanceName]bool),
	}
}

// active returns true while the operation may still be advanced.
func (op *resolveOpT) active() bool {
	switch op.getPhase() {
	case phaseDeleting, phaseCommitting, phaseMerging:
		return true
	default:
		return false
	}
}

// Synchronized access to phase and child.
func (op *resolveOpT) getPhase() resolvePhase {
	op.mu.RLock()
	p := op.phase
	op.mu.RUnlock()
	return p
}

func (op *resolveOpT) setPhase(p resolvePhase) {
	op.mu.Lock()
	op.phase = p
	op.mu.Unlock()
}

func (op *resolveOpT) getChild() dvid.UUID {
	op.mu.RLock()
	c := op.child
	op.mu.RUnlock()
	return c
}

// reserveChild returns the final child UUID, generating and recording it on
// first call.
func (op *resolveOpT) reserveChild() dvid.UUID {
	op.mu.Lock()
	if op.child == dvid.NilUUID {
		op.child = dvid.NewUUID()
	}
	c := op.child
	op.mu.Unlock()
	return c
}

// mergeParents returns the parents to use for the final merge: the extension
// node when one was created for a parent, otherwise the original parent.
func (op *resolveOpT) mergeParents() []dvid.UUID {
	merged := make([]dvid.UUID, len(op.parents))
	for i, old := range op.parents {
		if op.extensions[i] != dvid.NilUUID {
			merged[i] = op.extensions[i]
		} else {
			merged[i] = old
		}
	}
	return merged
}

// resolveOpKey returns the stable identity of a resolve operation: repo root,
// ordered parents, and the target instance set.  Identical submissions share a
// key; parent order or instance-set changes produce a different key.
func resolveOpKey(root dvid.UUID, parents []dvid.UUID, names []dvid.InstanceName) string {
	h := sha256.New()
	fmt.Fprintf(h, "root=%s\n", root)
	for _, p := range parents {
		fmt.Fprintf(h, "parent=%s\n", p)
	}
	sorted := append([]dvid.InstanceName(nil), names...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	for _, n := range sorted {
		fmt.Fprintf(h, "data=%s\n", n)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// Errors used to communicate operation disposition to callers.
var (
	// ErrResolveBadInput indicates a malformed or invalid request.
	ErrResolveBadInput = errors.New("invalid resolve request")

	// ErrResolveStale indicates the frozen plan no longer matches the repo.
	ErrResolveStale = errors.New("resolve operation is stale and cannot continue")

	// ErrResolveBusy indicates a different resolve is already active.
	ErrResolveBusy = errors.New("another resolve operation is in progress for repo")
)

// resolveHooks holds test-only fault injection points.  All fields default to
// nil; production code never changes behavior when unset.
var resolveHooks = struct {
	beforeScan   func(name dvid.InstanceName) error
	beforeDelete func(data DataService, extUUID dvid.UUID, k storage.Key) error
	beforeCommit func(extUUID dvid.UUID) error
	beforeMerge  func(child dvid.UUID) error
}{}

// ----- Public entry point -----

// Resolve freezes and executes, or resumes, a single conflict resolution
// operation.  Identical submissions (same repo root, same ordered parents,
// same target instances) share one operation record and one final child.
// The returned error is a bad-input error, ErrResolveStale, ErrResolveBusy,
// or a retryable phase-failure error.
func Resolve(uuid dvid.UUID, names []dvid.InstanceName, parentStrs []string, note string) (dvid.UUID, error) {
	m := getManager()
	if m == nil {
		return dvid.NilUUID, ErrManagerNotInitialized
	}

	root, err := GetRepoRoot(uuid)
	if err != nil {
		return dvid.NilUUID, fmt.Errorf("%w: %v", ErrResolveBadInput, err)
	}

	if len(names) == 0 {
		return dvid.NilUUID, fmt.Errorf("%w: must specify at least one data instance using 'data' field", ErrResolveBadInput)
	}
	if len(parentStrs) < 2 {
		return dvid.NilUUID, fmt.Errorf("%w: must specify at least two parent UUIDs using 'parents' field", ErrResolveBadInput)
	}

	// Resolve and validate parents: unique, committed, and in this repo.
	parents := make([]dvid.UUID, len(parentStrs))
	seen := make(map[dvid.UUID]struct{}, len(parentStrs))
	for i, frag := range parentStrs {
		u, _, err := MatchingUUID(frag)
		if err != nil {
			return dvid.NilUUID, fmt.Errorf("%w: can't match parent %q: %v", ErrResolveBadInput, frag, err)
		}
		pRoot, err := GetRepoRoot(u)
		if err != nil || pRoot != root {
			return dvid.NilUUID, fmt.Errorf("%w: parent %q is not in repo rooted at %s", ErrResolveBadInput, frag, root)
		}
		locked, err := LockedUUID(u)
		if err != nil {
			return dvid.NilUUID, fmt.Errorf("%w: %v", ErrResolveBadInput, err)
		}
		if !locked {
			return dvid.NilUUID, fmt.Errorf("%w: parent %s is not committed; commit all parents before resolve", ErrResolveBadInput, u)
		}
		if _, dup := seen[u]; dup {
			return dvid.NilUUID, fmt.Errorf("%w: duplicate parent %s in 'parents' field", ErrResolveBadInput, u)
		}
		seen[u] = struct{}{}
		parents[i] = u
	}

	// Validate target instances: unique names resolvable in this repo.
	namesSeen := make(map[dvid.InstanceName]struct{}, len(names))
	for _, name := range names {
		if _, dup := namesSeen[name]; dup {
			return dvid.NilUUID, fmt.Errorf("%w: duplicate data instance %q in 'data' field", ErrResolveBadInput, name)
		}
		namesSeen[name] = struct{}{}
		d, err := GetDataByUUIDName(uuid, name)
		if err != nil {
			return dvid.NilUUID, fmt.Errorf("%w: data instance %q: %v", ErrResolveBadInput, name, err)
		}
		_ = d
	}

	key := resolveOpKey(root, parents, names)

	r, err := m.repoFromUUID(root)
	if err != nil {
		return dvid.NilUUID, err
	}

	r.resolveMu.Lock()
	op, found := r.resolveOps[key]
	var phase resolvePhase
	if found {
		phase = op.getPhase()
	}
	switch {
	case found && phase == phaseStale:
		r.resolveMu.Unlock()
		if op.errMsg != "" {
			return dvid.NilUUID, fmt.Errorf("%w: %s", ErrResolveStale, op.errMsg)
		}
		return dvid.NilUUID, ErrResolveStale
	case found && phase == phaseDone:
		r.resolveMu.Unlock()
		if err := m.verifyFrozen(r, uuid, op); err != nil {
			return dvid.NilUUID, err
		}
		child := op.getChild()
		if _, err := m.versionFromUUID(child); err != nil {
			return dvid.NilUUID, fmt.Errorf("%w: final child %s no longer exists", ErrResolveStale, child)
		}
		return child, nil
	case found && (phase == phaseDeleting || phase == phaseCommitting || phase == phaseMerging):
		if _, running := r.resolveRunning[key]; running {
			r.resolveMu.Unlock()
			// Concurrent identical submission waits for and reuses the result.
			return m.waitResolve(r, key, uuid)
		}
		// Process exited mid-op previously; fall through to resume.
	default:
		for _, other := range r.resolveOps {
			if other.active() {
				r.resolveMu.Unlock()
				return dvid.NilUUID, fmt.Errorf("%w: operation %s", ErrResolveBusy, other.id)
			}
		}
		op = newResolveOp(dvid.NewUUID(), parents, names, note)
		if err := snapshotFingerprint(m, r, uuid, op); err != nil {
			r.resolveMu.Unlock()
			return dvid.NilUUID, err
		}
		r.resolveOps[key] = op
		// Persist the frozen plan before any extension or deletion exists.
		if err := r.save(); err != nil {
			r.resolveMu.Unlock()
			return dvid.NilUUID, err
		}
	}
	if r.resolveRunning == nil {
		r.resolveRunning = make(map[string]struct{})
	}
	if r.resolveCond == nil {
		r.resolveCond = sync.NewCond(&r.resolveMu)
	}
	r.resolveRunning[key] = struct{}{}
	r.resolveMu.Unlock()

	defer func() {
		r.resolveMu.Lock()
		delete(r.resolveRunning, key)
		if r.resolveCond != nil {
			r.resolveCond.Broadcast()
		}
		r.resolveMu.Unlock()
	}()

	return m.runResolve(r, key, uuid, op)
}

// waitResolve blocks until the in-flight operation for key finishes, then
// returns its final child.  If the runner failed, the waiter retries the
// operation (becoming the next runner).
func (m *repoManager) waitResolve(r *repoT, key string, urlUUID dvid.UUID) (dvid.UUID, error) {
	r.resolveMu.Lock()
	if r.resolveCond == nil {
		r.resolveCond = sync.NewCond(&r.resolveMu)
	}
	for {
		op := r.resolveOps[key]
		_, running := r.resolveRunning[key]
		if !running {
			if op != nil && op.getPhase() == phaseDone {
				child := op.getChild()
				r.resolveMu.Unlock()
				return child, nil
			}
			r.resolveMu.Unlock()
			if op == nil {
				return dvid.NilUUID, fmt.Errorf("resolve operation vanished while waiting")
			}
			// Retry with the stored, frozen inputs.
			parentStrs := make([]string, len(op.parents))
			for i, p := range op.parents {
				parentStrs[i] = string(p)
			}
			return Resolve(urlUUID, op.data, parentStrs, op.note)
		}
		r.resolveCond.Wait()
	}
}

// ----- Frozen fingerprint -----

// snapshotFingerprint captures the state that must remain stable for the plan
// to be valid: each parent's lock and children set, each instance's identity.
func snapshotFingerprint(m *repoManager, r *repoT, urlUUID dvid.UUID, op *resolveOpT) error {
	op.parentChildren = make([][]dvid.VersionID, len(op.parents))
	for i, p := range op.parents {
		v, err := m.versionFromUUID(p)
		if err != nil {
			return err
		}
		r.dag.RLock()
		node, found := r.dag.nodes[v]
		r.dag.RUnlock()
		if !found {
			return ErrInvalidVersion
		}
		node.RLock()
		kids := append([]dvid.VersionID{}, node.children...)
		node.RUnlock()
		if kids == nil {
			kids = []dvid.VersionID{}
		}
		op.parentChildren[i] = kids
	}

	op.instID = make([]dvid.InstanceID, len(op.data))
	op.instDataUUID = make([]dvid.UUID, len(op.data))
	for i, name := range op.data {
		d, err := m.getDataByUUIDName(urlUUID, name)
		if err != nil {
			return err
		}
		op.instID[i] = d.InstanceID()
		op.instDataUUID[i] = d.DataUUID()
	}
	return nil
}

// verifyFrozen checks the plan against current repo state.  Children a parent
// gained through this operation's own extensions are expected, not conflicts.
func (m *repoManager) verifyFrozen(r *repoT, urlUUID dvid.UUID, op *resolveOpT) error {
	for i, p := range op.parents {
		v, err := m.versionFromUUID(p)
		if err != nil {
			return m.markStale(r, op, "parent %s no longer exists", p)
		}
		r.dag.RLock()
		node, found := r.dag.nodes[v]
		r.dag.RUnlock()
		if !found {
			return m.markStale(r, op, "parent %s node missing from DAG", p)
		}
		node.RLock()
		locked := node.locked
		current := append([]dvid.VersionID{}, node.children...)
		node.RUnlock()
		if !locked {
			return m.markStale(r, op, "parent %s is no longer committed", p)
		}

		expected := append([]dvid.VersionID{}, op.parentChildren[i]...)
		if op.extensions[i] != dvid.NilUUID {
			if extV, err := m.versionFromUUID(op.extensions[i]); err == nil {
				expected = append(expected, extV)
			}
		}
		// The final merge child is itself a child of any direct merge parent.
		if child := op.getChild(); child != dvid.NilUUID {
			if childV, err := m.versionFromUUID(child); err == nil {
				r.dag.RLock()
				cn, ok := r.dag.nodes[childV]
				r.dag.RUnlock()
				if ok {
					for _, cpv := range cn.parents {
						if cpv == v {
							expected = append(expected, childV)
							break
						}
					}
				}
			}
		}
		if !sameVersionSet(expected, current) {
			return m.markStale(r, op, "parent %s gained children outside this resolve operation", p)
		}
	}

	for i, name := range op.data {
		d, err := m.getDataByUUIDName(urlUUID, name)
		if err != nil {
			return m.markStale(r, op, "target instance %q no longer exists", name)
		}
		if d.InstanceID() != op.instID[i] || d.DataUUID() != op.instDataUUID[i] {
			return m.markStale(r, op, "target instance %q identity changed", name)
		}
	}
	return nil
}

func sameVersionSet(a, b []dvid.VersionID) bool {
	if len(a) != len(b) {
		return false
	}
	ac := append([]dvid.VersionID{}, a...)
	bc := append([]dvid.VersionID{}, b...)
	sort.Slice(ac, func(i, j int) bool { return ac[i] < ac[j] })
	sort.Slice(bc, func(i, j int) bool { return bc[i] < bc[j] })
	for i := range ac {
		if ac[i] != bc[i] {
			return false
		}
	}
	return true
}

// markStale makes the operation a terminal stale record and persists it.
func (m *repoManager) markStale(r *repoT, op *resolveOpT, format string, args ...interface{}) error {
	op.errMsg = fmt.Sprintf(format, args...)
	op.updated = time.Now().Round(0)
	op.setPhase(phaseStale)
	if err := r.save(); err != nil {
		return fmt.Errorf("%w: %s (also failed to persist stale state: %v)", ErrResolveStale, op.errMsg, err)
	}
	return fmt.Errorf("%w: %s", ErrResolveStale, op.errMsg)
}

// ----- Extension nodes -----

// resolveExtension returns the single extension node for a parent, creating it
// (idempotently with a pre-assigned UUID) on first conflict.
func (m *repoManager) resolveExtension(r *repoT, op *resolveOpT, idx int) (dvid.UUID, dvid.VersionID, error) {
	if op.extensions[idx] == dvid.NilUUID {
		op.extensions[idx] = dvid.NewUUID()
	}
	extUUID := op.extensions[idx]
	branch := fmt.Sprintf("resolve-%.8s-%d", extUUID, idx)
	if _, err := m.newVersionAssigned(op.parents[idx], extUUID, "Version for deleting conflicts before merge", branch); err != nil {
		return dvid.NilUUID, 0, err
	}
	extV, err := m.versionFromUUID(extUUID)
	if err != nil {
		return dvid.NilUUID, 0, err
	}
	return extUUID, extV, nil
}

// ----- Deletion phase -----

func (m *repoManager) runResolve(r *repoT, key string, urlUUID dvid.UUID, op *resolveOpT) (dvid.UUID, error) {
	if err := m.verifyFrozen(r, urlUUID, op); err != nil {
		return dvid.NilUUID, err
	}

	if op.phase == phaseDeleting {
		for _, name := range op.data {
			if op.instDone[name] {
				continue
			}
			if resolveHooks.beforeScan != nil {
				if err := resolveHooks.beforeScan(name); err != nil {
					op.errMsg = err.Error()
					op.updated = time.Now().Round(0)
					_ = r.save()
					return dvid.NilUUID, fmt.Errorf("resolve scan error for data %q: %w", name, err)
				}
			}
			data, err := m.getDataByUUIDName(urlUUID, name)
			if err != nil {
				return dvid.NilUUID, err
			}
			if err := m.deleteInstanceConflicts(r, op, data); err != nil {
				op.errMsg = err.Error()
				op.updated = time.Now().Round(0)
				_ = r.save()
				return dvid.NilUUID, fmt.Errorf("conflict deletion error for data %q: %w", name, err)
			}
			op.instDone[name] = true
			op.updated = time.Now().Round(0)
			if err := r.save(); err != nil {
				return dvid.NilUUID, err
			}
			// A state change outside this operation during the scan invalidates
			// everything that follows.
			if err := m.verifyFrozen(r, urlUUID, op); err != nil {
				return dvid.NilUUID, err
			}
		}
		op.setPhase(phaseCommitting)
		op.updated = time.Now().Round(0)
		if err := r.save(); err != nil {
			return dvid.NilUUID, err
		}
	}

	// Commit phase: lock each used extension node.
	if op.phase == phaseCommitting {
		for i, extUUID := range op.extensions {
			if extUUID == dvid.NilUUID || op.extCommitted[i] {
				continue
			}
			extV, err := m.versionFromUUID(extUUID)
			if err != nil {
				return dvid.NilUUID, err
			}
			r.dag.RLock()
			node, found := r.dag.nodes[extV]
			r.dag.RUnlock()
			if !found {
				return dvid.NilUUID, ErrInvalidVersion
			}
			node.RLock()
			locked := node.locked
			node.RUnlock()
			if !locked {
				if resolveHooks.beforeCommit != nil {
					if err := resolveHooks.beforeCommit(extUUID); err != nil {
						op.errMsg = err.Error()
						op.updated = time.Now().Round(0)
						_ = r.save()
						return dvid.NilUUID, fmt.Errorf("resolve commit error for extension %s: %w", extUUID, err)
					}
				}
				if err := m.commit(extUUID, "Version for deleting conflicts before merge", nil); err != nil {
					op.errMsg = err.Error()
					op.updated = time.Now().Round(0)
					_ = r.save()
					return dvid.NilUUID, fmt.Errorf("resolve commit error for extension %s: %w", extUUID, err)
				}
			}
			op.extCommitted[i] = true
			op.updated = time.Now().Round(0)
			if err := r.save(); err != nil {
				return dvid.NilUUID, err
			}
		}
		op.setPhase(phaseMerging)
		op.updated = time.Now().Round(0)
		if err := r.save(); err != nil {
			return dvid.NilUUID, err
		}
	}

	// Merge phase: reserved, assigned, idempotent final merge.
	if op.getPhase() == phaseMerging {
		// Persist the reserved final child before performing the merge.
		op.reserveChild()
		op.updated = time.Now().Round(0)
		if err := r.save(); err != nil {
			return dvid.NilUUID, err
		}
		finalChild := op.getChild()
		if resolveHooks.beforeMerge != nil {
			if err := resolveHooks.beforeMerge(finalChild); err != nil {
				op.errMsg = err.Error()
				op.updated = time.Now().Round(0)
				_ = r.save()
				return dvid.NilUUID, fmt.Errorf("resolve merge error for child %s: %w", finalChild, err)
			}
		}
		if err := m.mergeAssigned(op.mergeParents(), finalChild, op.note); err != nil {
			op.errMsg = err.Error()
			op.updated = time.Now().Round(0)
			_ = r.save()
			return dvid.NilUUID, fmt.Errorf("resolve merge error for child %s: %w", finalChild, err)
		}
		op.errMsg = ""
		op.setPhase(phaseDone)
		op.updated = time.Now().Round(0)
		if err := r.save(); err != nil {
			return dvid.NilUUID, err
		}
	}

	return op.getChild(), nil
}

// deleteInstanceConflicts scans one target instance and tombstones every
// conflicting kv according to parent priority.  All per-key errors are
// collected and returned rather than silently ignored.
func (m *repoManager) deleteInstanceConflicts(r *repoT, op *resolveOpT, data DataService) error {
	parentsV := make([]dvid.VersionID, len(op.parents))
	parentIdx := make(map[dvid.VersionID]int, len(op.parents))
	for i, p := range op.parents {
		v, err := m.versionFromUUID(p)
		if err != nil {
			return err
		}
		parentsV[i] = v
		parentIdx[v] = i
	}

	store, err := GetOrderedKeyValueDB(data)
	if err != nil {
		return err
	}

	var (
		errMu sync.Mutex
		errs  []error
	)
	addErr := func(err error) {
		errMu.Lock()
		errs = append(errs, err)
		errMu.Unlock()
	}

	baseCtx := NewVersionedCtx(data, 0)
	ch := make(chan *storage.KeyValue, 1000)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()

		var curV dvid.VersionID
		var curTK, batchTK storage.TKey
		kvv := kvVersions{}
		for {
			kv := <-ch
			if kv == nil {
				curTK = nil
			} else {
				var err error
				curV, err = baseCtx.VersionFromKey(kv.K)
				if err != nil {
					addErr(fmt.Errorf("can't decode key when deleting conflicts for %s: %v", data.DataName(), err))
					continue
				}
				curTK, err = storage.TKeyFromKey(kv.K)
				if err != nil {
					addErr(fmt.Errorf("error processing kv pairs in resolve for %s: %v", data.DataName(), err))
					continue
				}
				if batchTK == nil {
					batchTK = curTK
				}
			}
			if !bytes.Equal(curTK, batchTK) {
				toDelete, err := kvv.FindConflicts(parentsV)
				if err != nil {
					addErr(err)
				} else {
					for oldV, k := range toDelete {
						extUUID, extV, err := m.resolveExtension(r, op, parentIdx[oldV])
						if err != nil {
							addErr(fmt.Errorf("unable to create extension node: %v", err))
							continue
						}
						if resolveHooks.beforeDelete != nil {
							if err := resolveHooks.beforeDelete(data, extUUID, k); err != nil {
								addErr(fmt.Errorf("unable to delete conflict: %v", err))
								continue
							}
						}
						tk, err := storage.TKeyFromKey(k)
						if err != nil {
							addErr(err)
							continue
						}
						ctx := NewVersionedCtx(data, extV)
						if err := store.Delete(ctx, tk); err != nil {
							addErr(fmt.Errorf("unable to delete conflict: %v", err))
						}
					}
				}

				kvv = kvVersions{}
				batchTK = curTK
			}
			if kv == nil {
				return
			}
			kvv[curV] = kvvNode{kv: kv}
		}
	}()

	minKey, maxKey := baseCtx.KeyRange()
	if err := store.RawRangeQuery(minKey, maxKey, true, ch, nil); err != nil {
		return err
	}
	wg.Wait()
	if len(errs) != 0 {
		return errors.Join(errs...)
	}
	return nil
}

// ----- Gob serialization (field order is part of the persisted format) -----

func (op *resolveOpT) GobEncode() ([]byte, error) {
	op.mu.RLock()
	defer op.mu.RUnlock()

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	fields := []interface{}{
		op.id,
		op.parents,
		op.data,
		op.note,
		op.created,
		op.updated,
		uint8(op.phase),
		op.extensions,
		op.extCommitted,
		op.instDone,
		op.child,
		op.parentChildren,
		op.instID,
		op.instDataUUID,
		op.errMsg,
	}
	for _, f := range fields {
		if err := enc.Encode(f); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (op *resolveOpT) GobDecode(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)

	var phase uint8
	fields := []interface{}{
		&op.id,
		&op.parents,
		&op.data,
		&op.note,
		&op.created,
		&op.updated,
		&phase,
		&op.extensions,
		&op.extCommitted,
		&op.instDone,
		&op.child,
		&op.parentChildren,
		&op.instID,
		&op.instDataUUID,
		&op.errMsg,
	}
	for _, f := range fields {
		if err := dec.Decode(f); err != nil {
			return err
		}
	}
	op.phase = resolvePhase(phase)
	if op.instDone == nil {
		op.instDone = make(map[dvid.InstanceName]bool)
	}
	// gob turns zero-length slices into nil; normalize so an empty child set
	// is round-tripped as a non-nil empty slice.
	if op.parentChildren != nil {
		for i, cs := range op.parentChildren {
			if cs == nil {
				op.parentChildren[i] = []dvid.VersionID{}
			}
		}
	}
	return nil
}
