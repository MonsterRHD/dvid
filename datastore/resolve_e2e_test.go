//go:build !clustered && !gcloud
// +build !clustered,!gcloud

package datastore

import (
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// instanceDataSnapshot enumerates component key -> value pairs (data markers
// only) across all versions of an instance.
func instanceDataSnapshot(t *testing.T, data DataService) map[string]string {
	t.Helper()
	store, err := GetOrderedKeyValueDB(data)
	if err != nil {
		t.Fatal(err)
	}
	baseCtx := NewVersionedCtx(data, 0)
	minKey, maxKey := baseCtx.KeyRange()

	out := make(map[string]string)
	ch := make(chan *storage.KeyValue, 100)
	go func() {
		if err := store.RawRangeQuery(minKey, maxKey, false, ch, nil); err != nil {
			t.Error(err)
		}
	}()
	for kv := range ch {
		if kv == nil {
			break
		}
		if kv.K.IsTombstone() {
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
		out[string(component)] = string(kv.V)
	}
	return out
}

// TestResolveEndToEndRestart verifies the full recovery flow: instance 1
// completes, instance 2 fails; after a process restart the retry completes
// with a usable child, and the unselected instance is untouched.
func TestResolveEndToEndRestart(t *testing.T) {
	OpenTest()
	defer CloseTest()

	root, err := NewRepo("resolveE2E", "e2e repo", nil, "")
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
	d3, err := NewData(root, tt, "resdata3", dvid.NewConfig())
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

	// Selected instances: overlapping conflicts like the standard env.
	for _, d := range []DataService{d1, d2} {
		putResolveKV(t, d, p1V, "a", "a-p1")
		putResolveKV(t, d, p2V, "a", "a-p2")
		putResolveKV(t, d, p2V, "b", "b-p2")
		putResolveKV(t, d, p1V, "c", "c-p1")
		putResolveKV(t, d, p2V, "c", "c-p2")
	}
	// Unselected instance: unrelated keys that must remain byte-identical.
	putResolveKV(t, d3, p1V, "x1", "x1-val")
	putResolveKV(t, d3, p2V, "x2", "x2-val")

	if err := Commit(p1, "p1 commit", nil); err != nil {
		t.Fatal(err)
	}
	if err := Commit(p2, "p2 commit", nil); err != nil {
		t.Fatal(err)
	}

	beforeUnselected := instanceDataSnapshot(t, d3)

	// Fail the scan of the second instance once.
	var failed int32
	resolveHooks.beforeScan = func(name dvid.InstanceName) error {
		if name == "resdata2" && atomic.CompareAndSwapInt32(&failed, 0, 1) {
			return errInjectedE2E
		}
		return nil
	}

	resolveReq := func() (dvid.UUID, error) {
		return Resolve(root,
			[]dvid.InstanceName{"resdata1", "resdata2"},
			[]string{string(p1), string(p2)},
			"e2e resolve")
	}

	_, err = resolveReq()
	if err == nil {
		t.Fatal("expected injected failure")
	}
	m := getManager()
	r, err := m.repoFromUUID(root)
	if err != nil {
		t.Fatal(err)
	}
	opKey := resolveOpKey(root, []dvid.UUID{p1, p2}, []dvid.InstanceName{"resdata1", "resdata2"})
	op := r.resolveOps[opKey]
	if op.instDone["resdata1"] != true || op.instDone["resdata2"] != false {
		t.Fatalf("unexpected progress: %v", op.instDone)
	}

	// Unselected instance unchanged after the partial run.
	if got := instanceDataSnapshot(t, d3); !reflect.DeepEqual(got, beforeUnselected) {
		t.Fatalf("unselected instance changed during partial resolve: %v vs %v", got, beforeUnselected)
	}

	// Process restart, then retry the same operation.
	resolveHooks.beforeScan = nil
	CloseReopenTest()

	child, err := resolveReq()
	if err != nil {
		t.Fatalf("resolve failed after restart: %v", err)
	}

	m2 := getManager()
	r2, err := m2.repoFromUUID(root)
	if err != nil {
		t.Fatal(err)
	}
	op2 := r2.resolveOps[opKey]
	if op2.phase != phaseDone || op2.child != child {
		t.Fatalf("operation not completed correctly: %+v", op2)
	}

	// Unselected instance still untouched and carries no tombstones.
	d3fresh, err := GetDataByUUIDName(root, "resdata3")
	if err != nil {
		t.Fatal(err)
	}
	if got := instanceDataSnapshot(t, d3fresh); !reflect.DeepEqual(got, beforeUnselected) {
		t.Fatalf("unselected instance changed across resolve: %v vs %v", got, beforeUnselected)
	}

	// Final child is a normal usable version: commit it, then branch and commit.
	if err := Commit(child, "resolved merge ready", nil); err != nil {
		t.Fatalf("can't commit final child: %v", err)
	}
	grandchild, err := NewVersion(child, "child of resolved merge", "postresolve", nil)
	if err != nil {
		t.Fatalf("can't branch final child: %v", err)
	}
	if err := Commit(grandchild, "post resolve commit", nil); err != nil {
		t.Fatalf("can't commit branch off child: %v", err)
	}

	// Priority reads at the child.
	childV, err := m2.versionFromUUID(child)
	if err != nil {
		t.Fatal(err)
	}
	d1fresh, err := GetDataByUUIDName(root, "resdata1")
	if err != nil {
		t.Fatal(err)
	}
	d2fresh, err := GetDataByUUIDName(root, "resdata2")
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range []DataService{d1fresh, d2fresh} {
		expect := map[string]string{"a": "a-p1", "b": "b-p2", "c": "c-p1"}
		for k, v := range expect {
			got, found := valueAt(t, d, childV, k)
			if !found || got != v {
				t.Fatalf("child read %q on %s: expected %q got %q", k, d.DataName(), v, got)
			}
		}
	}
}

var errInjectedE2E = errors.New("injected e2e failure")
