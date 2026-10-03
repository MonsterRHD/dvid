//go:build !clustered && !gcloud
// +build !clustered,!gcloud

package datastore

import (
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

func init() {
	Register(&TestType{Type: Type{
		Name:    "resolvetest",
		URL:     "github.com/janelia-flyem/dvid/tests/resolvetest",
		Version: "1.0",
	}})
}

type resolveTestEnv struct {
	root       dvid.UUID
	p1, p2     dvid.UUID
	d1, d2     DataService
	opKey      string
	p1V, p2V   dvid.VersionID
}

// buildResolveTestEnv creates a repo with two committed branches holding
// overlapping + unique keys in two identical test instances.
func buildResolveTestEnv(t *testing.T) *resolveTestEnv {
	t.Helper()
	OpenTest()

	root, err := NewRepo("resolveTest", "resolve test repository", nil, "")
	if err != nil {
		t.Fatal(err)
	}

	tt, err := TypeServiceByName("resolvetest")
	if err != nil {
		t.Fatal(err)
	}
	d1, err := NewData(root, tt, "resdata1", dvid.NewConfig())
	if err != nil {
		t.Fatal(err)
	}
	d2, err := NewData(root, tt, "resdata2", dvid.NewConfig())
	if err != nil {
		t.Fatal(err)
	}

	if err := Commit(root, "root commit", nil); err != nil {
		t.Fatal(err)
	}

	p1, err := NewVersion(root, "p1", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	p2, err := NewVersion(root, "p2", "p2branch", nil)
	if err != nil {
		t.Fatal(err)
	}

	p1V, err := VersionFromUUID(p1)
	if err != nil {
		t.Fatal(err)
	}
	p2V, err := VersionFromUUID(p2)
	if err != nil {
		t.Fatal(err)
	}

	// Per-instance data:
	// a: p1 & p2 differ -> conflict, p2 deleted (p1 priority)
	// b: only on p2 -> retained
	// c: p1 & p2 differ -> conflict, p2 deleted
	// d: only on p1 -> retained
	for _, d := range []DataService{d1, d2} {
		putResolveKV(t, d, p1V, "a", "a-p1")
		putResolveKV(t, d, p2V, "a", "a-p2")
		putResolveKV(t, d, p2V, "b", "b-p2")
		putResolveKV(t, d, p1V, "c", "c-p1")
		putResolveKV(t, d, p2V, "c", "c-p2")
		putResolveKV(t, d, p1V, "d", "d-p1")
	}

	if err := Commit(p1, "p1 commit", nil); err != nil {
		t.Fatal(err)
	}
	if err := Commit(p2, "p2 commit", nil); err != nil {
		t.Fatal(err)
	}

	root2, err := GetRepoRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	opKey := resolveOpKey(root2, []dvid.UUID{p1, p2}, []dvid.InstanceName{"resdata1", "resdata2"})

	return &resolveTestEnv{
		root: root2, p1: p1, p2: p2, d1: d1, d2: d2, opKey: opKey, p1V: p1V, p2V: p2V,
	}
}

func putResolveKV(t *testing.T, data DataService, v dvid.VersionID, key, val string) {
	t.Helper()
	store, err := GetOrderedKeyValueDB(data)
	if err != nil {
		t.Fatal(err)
	}
	ctx := NewVersionedCtx(data, v)
	tk := storage.NewTKey(storage.TKeyClass(1), []byte(key))
	if err := store.Put(ctx, tk, []byte(val)); err != nil {
		t.Fatalf("can't put test kv %q: %v", key, err)
	}
}

func getResolveRepo(t *testing.T, env *resolveTestEnv) (*repoManager, *repoT) {
	t.Helper()
	m := getManager()
	r, err := m.repoFromUUID(env.root)
	if err != nil {
		t.Fatal(err)
	}
	return m, r
}

func getResolveOp(t *testing.T, env *resolveTestEnv) (*repoManager, *repoT, *resolveOpT) {
	t.Helper()
	m, r := getResolveRepo(t, env)
	op, found := r.resolveOps[env.opKey]
	if !found {
		t.Fatal("resolve operation record missing")
	}
	return m, r, op
}

func countMergeNodes(r *repoT) int {
	count := 0
	for _, node := range r.dag.nodes {
		if len(node.parents) >= 2 {
			count++
		}
	}
	return count
}

func countResolveExtensions(r *repoT, op *resolveOpT) int {
	count := 0
	for _, ext := range op.extensions {
		if ext != dvid.NilUUID {
			count++
		}
	}
	return count
}

// tombstoneTKeys enumerates tombstoned component keys present at extV
// (badger test backend exposes tombstone keys during range queries).
func tombstoneTKeys(t *testing.T, data DataService, extUUID dvid.UUID) map[string]struct{} {
	t.Helper()
	m := getManager()
	extV, err := m.versionFromUUID(extUUID)
	if err != nil {
		t.Fatal(err)
	}
	store, err := GetOrderedKeyValueDB(data)
	if err != nil {
		t.Fatal(err)
	}
	baseCtx := NewVersionedCtx(data, 0)
	minKey, maxKey := baseCtx.KeyRange()

	out := make(map[string]struct{})
	ch := make(chan *storage.KeyValue, 100)
	go func() {
		if err := store.RawRangeQuery(minKey, maxKey, true, ch, nil); err != nil {
			t.Error(err)
		}
	}()
	for kv := range ch {
		if kv == nil {
			break
		}
		if !kv.K.IsTombstone() {
			continue
		}
		v, err := baseCtx.VersionFromKey(kv.K)
		if err != nil {
			t.Fatal(err)
		}
		if v != extV {
			continue
		}
		tk, err := storage.TKeyFromKey(kv.K)
		if err != nil {
			t.Fatal(err)
		}
		class, err := tk.Class()
		if err != nil {
			t.Fatal(err)
		}
		component, err := tk.ClassBytes(class)
		if err != nil {
			t.Fatal(err)
		}
		out[string(component)] = struct{}{}
	}
	return out
}

var expectedDeleted = map[string]struct{}{"a": {}, "c": {}}

func assertTombstoneSet(t *testing.T, got map[string]struct{}) {
	t.Helper()
	if len(got) != len(expectedDeleted) {
		t.Fatalf("expected %d tombstones %v, got %d %v", len(expectedDeleted), sortedKeys(expectedDeleted), len(got), sortedKeys(got))
	}
	for k := range expectedDeleted {
		if _, ok := got[k]; !ok {
			t.Fatalf("expected tombstone for %q, got %v", k, sortedKeys(got))
		}
	}
}

func sortedKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func callResolve(t *testing.T, env *resolveTestEnv) (dvid.UUID, error) {
	t.Helper()
	return Resolve(env.root,
		[]dvid.InstanceName{"resdata1", "resdata2"},
		[]string{string(env.p1), string(env.p2)},
		"resolve test note")
}

// valueAt performs the same versioned lookup a GET does: scan all kv for the
// given component key, then FindMatch at childV.
func valueAt(t *testing.T, data DataService, childV dvid.VersionID, key string) (string, bool) {
	t.Helper()
	store, err := GetOrderedKeyValueDB(data)
	if err != nil {
		t.Fatal(err)
	}
	baseCtx := NewVersionedCtx(data, 0)
	minKey, maxKey := baseCtx.KeyRange()

	ch := make(chan *storage.KeyValue, 100)
	go func() {
		if err := store.RawRangeQuery(minKey, maxKey, false, ch, nil); err != nil {
			t.Error(err)
		}
	}()
	kvv := kvVersions{}
	for kv := range ch {
		if kv == nil {
			break
		}
		tk, err := storage.TKeyFromKey(kv.K)
		if err != nil {
			t.Fatal(err)
		}
		class, err := tk.Class()
		if err != nil {
			t.Fatal(err)
		}
		component, err := tk.ClassBytes(class)
		if err != nil {
			t.Fatal(err)
		}
		if string(component) != key {
			continue
		}
		v, err := baseCtx.VersionFromKey(kv.K)
		if err != nil {
			t.Fatal(err)
		}
		kvv[v] = kvvNode{kv: kv}
	}
	match, _, err := kvv.FindMatch(childV)
	if err != nil {
		t.Fatalf("error finding match for %q at %d: %v", key, childV, err)
	}
	if match == nil {
		return "", false
	}
	return string(match.V), true
}

// assertChildReads checks the priority-resolved values at the final child.
func assertChildReads(t *testing.T, env *resolveTestEnv, child dvid.UUID) {
	t.Helper()
	m := getManager()
	childV, err := m.versionFromUUID(child)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range []DataService{env.d1, env.d2} {
		expect := map[string]string{"a": "a-p1", "b": "b-p2", "c": "c-p1", "d": "d-p1"}
		for k, v := range expect {
			got, found := valueAt(t, d, childV, k)
			if !found || got != v {
				t.Errorf("child read key %q on %s: expected %q, got %q (found %t)", k, d.DataName(), v, got, found)
			}
		}
	}
}

func TestResolveHappyPath(t *testing.T) {
	env := buildResolveTestEnv(t)
	defer CloseTest()

	child, err := callResolve(t, env)
	if err != nil {
		t.Fatalf("expected successful resolve, got %v", err)
	}
	if child == dvid.NilUUID {
		t.Fatal("got nil child from resolve")
	}

	m, r, op := getResolveOp(t, env)
	if op.phase != phaseDone {
		t.Fatalf("expected phase done, got %d", op.phase)
	}
	if !op.instDone["resdata1"] || !op.instDone["resdata2"] {
		t.Fatalf("expected both instances done, got %v", op.instDone)
	}
	if op.extensions[0] != dvid.NilUUID {
		t.Fatal("expected no extension for p1 (higher priority)")
	}
	if op.extensions[1] == dvid.NilUUID {
		t.Fatal("expected extension for p2")
	}
	if countResolveExtensions(r, op) != 1 {
		t.Fatal("expected exactly one extension node")
	}

	// Extension must be committed before the merge.
	extV, err := m.versionFromUUID(op.extensions[1])
	if err != nil {
		t.Fatal(err)
	}
	if !r.dag.nodes[extV].locked {
		t.Fatal("extension node not committed before merge")
	}

	// Exactly one merge node, with parents p1 and the p2 extension.
	if countMergeNodes(r) != 1 {
		t.Fatalf("expected exactly one merge node, got %d", countMergeNodes(r))
	}
	childV, _ := m.versionFromUUID(child)
	mergeNode := r.dag.nodes[childV]
	if !sameVersionSet(mergeNode.parents, []dvid.VersionID{env.p1V, extV}) {
		t.Fatalf("merge node parents %v unexpected", mergeNode.parents)
	}

	assertTombstoneSet(t, tombstoneTKeys(t, env.d1, op.extensions[1]))
	assertTombstoneSet(t, tombstoneTKeys(t, env.d2, op.extensions[1]))
	assertChildReads(t, env, child)

	// Duplicate submission returns same child without any new nodes.
	child2, err := callResolve(t, env)
	if err != nil {
		t.Fatalf("duplicate resolve failed: %v", err)
	}
	if child2 != child {
		t.Fatal("duplicate resolve returned different child")
	}
	_, r2, op2 := getResolveOp(t, env)
	if countResolveExtensions(r2, op2) != 1 || countMergeNodes(r2) != 1 {
		t.Fatal("duplicate resolve created additional nodes")
	}
}

func TestResolveScanFailure(t *testing.T) {
	env := buildResolveTestEnv(t)
	defer CloseTest()

	var failed int32
	resolveHooks.beforeScan = func(name dvid.InstanceName) error {
		if name == "resdata2" && atomic.CompareAndSwapInt32(&failed, 0, 1) {
			return errors.New("injected scan failure")
		}
		return nil
	}
	defer func() { resolveHooks.beforeScan = nil }()

	_, err := callResolve(t, env)
	if err == nil {
		t.Fatal("expected scan failure error")
	}
	_, r, op := getResolveOp(t, env)
	if op.phase != phaseDeleting {
		t.Fatalf("expected phase deleting after scan failure, got %d", op.phase)
	}
	if !op.instDone["resdata1"] || op.instDone["resdata2"] {
		t.Fatalf("unexpected instance done state: %v", op.instDone)
	}
	if op.extensions[1] == dvid.NilUUID {
		t.Fatal("expected extension from completed instance to exist")
	}
	if countMergeNodes(r) != 0 {
		t.Fatal("no merge node may exist after a failure")
	}

	// Simulate process restart, then retry to full completion.
	CloseReopenTest()
	child, err := callResolve(t, env)
	if err != nil {
		t.Fatalf("resolve failed after restart: %v", err)
	}
	_, r2, op2 := getResolveOp(t, env)
	if op2.phase != phaseDone {
		t.Fatalf("expected phase done after resumed run, got %d", op2.phase)
	}
	if op2.extensions[1] != op.extensions[1] {
		t.Fatal("resume after restart created a new extension")
	}
	if countMergeNodes(r2) != 1 {
		t.Fatal("expected single merge node after restart resume")
	}
	d1, err := GetDataByUUIDName(env.root, "resdata1")
	if err != nil {
		t.Fatal(err)
	}
	d2, err := GetDataByUUIDName(env.root, "resdata2")
	if err != nil {
		t.Fatal(err)
	}
	env.d1, env.d2 = d1, d2
	assertTombstoneSet(t, tombstoneTKeys(t, d1, op2.extensions[1]))
	assertTombstoneSet(t, tombstoneTKeys(t, d2, op2.extensions[1]))
	assertChildReads(t, env, child)
}

func TestResolveDeleteFailure(t *testing.T) {
	env := buildResolveTestEnv(t)
	defer CloseTest()

	var failed int32
	resolveHooks.beforeDelete = func(data DataService, extUUID dvid.UUID, k storage.Key) error {
		tk, err := storage.TKeyFromKey(k)
		if err != nil {
			return err
		}
		class, err := tk.Class()
		if err != nil {
			return err
		}
		component, err := tk.ClassBytes(class)
		if err != nil {
			return err
		}
		if string(component) == "a" && atomic.CompareAndSwapInt32(&failed, 0, 1) {
			return fmt.Errorf("injected delete failure")
		}
		return nil
	}
	defer func() { resolveHooks.beforeDelete = nil }()

	_, err := callResolve(t, env)
	if err == nil {
		t.Fatal("expected delete failure error")
	}
	_, _, op := getResolveOp(t, env)
	if op.phase != phaseDeleting || op.instDone["resdata1"] {
		t.Fatalf("unexpected state after delete failure: phase %d done %v", op.phase, op.instDone)
	}
	if op.extensions[1] == dvid.NilUUID {
		t.Fatal("extension should have been created before the failed delete")
	}

	// Retry completes with the same extension and correct final child.
	child, err := callResolve(t, env)
	if err != nil {
		t.Fatalf("resolve failed after delete retry: %v", err)
	}
	_, r2, op2 := getResolveOp(t, env)
	if op2.extensions[1] != op.extensions[1] {
		t.Fatal("retry created a new extension after delete failure")
	}
	if countMergeNodes(r2) != 1 {
		t.Fatal("expected single merge node after delete retry")
	}
	assertTombstoneSet(t, tombstoneTKeys(t, env.d1, op2.extensions[1]))
	assertChildReads(t, env, child)
}
