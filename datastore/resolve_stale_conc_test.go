//go:build !clustered && !gcloud
// +build !clustered,!gcloud

package datastore

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
)

// expectStale asserts err wraps ErrResolveStale.
func expectStale(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrResolveStale) {
		t.Fatalf("expected ErrResolveStale, got %v", err)
	}
}

// TestResolveStaleForeignChild: an unrelated committed child appearing on a
// parent during the op invalidates the plan permanently.
func TestResolveStaleForeignChild(t *testing.T) {
	env := buildResolveTestEnv(t)
	defer CloseTest()

	var foreignOnce sync.Once
	resolveHooks.beforeScan = func(name dvid.InstanceName) error {
		if name != "resdata1" {
			return nil
		}
		foreignOnce.Do(func() {
			foreign, err := NewVersion(env.p2, "foreign branch child", "foreignbranch", nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := Commit(foreign, "foreign commit", nil); err != nil {
				t.Fatal(err)
			}
		})
		return nil
	}
	defer func() { resolveHooks.beforeScan = nil }()

	_, err := callResolve(t, env)
	expectStale(t, err)

	_, _, op := getResolveOp(t, env)
	if op.phase != phaseStale {
		t.Fatalf("expected stale terminal phase, got %d", op.phase)
	}
	// Repeated identical submissions keep failing.
	_, err = callResolve(t, env)
	expectStale(t, err)

	// A fresh, distinct plan (reversed priority) still works independently.
	revChild, err := Resolve(env.root,
		[]dvid.InstanceName{"resdata1", "resdata2"},
		[]string{string(env.p2), string(env.p1)},
		"reverse plan")
	if err != nil {
		t.Fatalf("new plan should succeed after stale: %v", err)
	}
	if revChild == dvid.NilUUID {
		t.Fatal("nil child from new plan")
	}
	m := getManager()
	rv, err := m.versionFromUUID(revChild)
	if err != nil {
		t.Fatal(err)
	}
	r, err := m.repoFromUUID(env.root)
	if err != nil {
		t.Fatal(err)
	}
	if r.dag.nodes[rv] == nil || len(r.dag.nodes[rv].parents) != 2 {
		t.Fatal("new plan did not produce a valid merge node")
	}
}

// TestResolveStaleInstanceChanged: rename or simulated deletion of a target
// instance during the op invalidates the plan.
func TestResolveStaleInstanceChanged(t *testing.T) {
	for _, mode := range []string{"rename", "delete"} {
		t.Run(mode, func(t *testing.T) {
			env := buildResolveTestEnv(t)
			defer CloseTest()

			resolveHooks.beforeScan = func(name dvid.InstanceName) error {
				if name != "resdata1" {
					return nil
				}
				m := getManager()
				r, err := m.repoFromUUID(env.root)
				if err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "rename":
					if err := RenameData(env.root, "resdata2", "resdata2renamed", ""); err != nil {
						t.Fatal(err)
					}
				case "delete":
					// Simulate the post-delete state: instance absent from repo.
					r.Lock()
					delete(r.data, "resdata2")
					r.Unlock()
				}
				return nil
			}
			defer func() { resolveHooks.beforeScan = nil }()

			_, err := callResolve(t, env)
			expectStale(t, err)
			_, _, op := getResolveOp(t, env)
			if op.phase != phaseStale {
				t.Fatal("op not persisted as stale")
			}
		})
	}
}

// holdResolveAtScan installs a barrier that pauses the op on the first scan,
// returning channels to detect and release it.
func holdResolveAtScan(t *testing.T, env *resolveTestEnv) (started, release chan struct{}) {
	t.Helper()
	started = make(chan struct{})
	release = make(chan struct{})
	var once sync.Once
	resolveHooks.beforeScan = func(name dvid.InstanceName) error {
		if name == "resdata1" {
			once.Do(func() { close(started) })
			<-release
		}
		return nil
	}
	return
}

// TestResolveConcurrentIdentical: concurrent identical submissions all
// resolve to the same single child without extra nodes.
func TestResolveConcurrentIdentical(t *testing.T) {
	env := buildResolveTestEnv(t)
	defer CloseTest()

	started, release := holdResolveAtScan(t, env)
	defer func() {
		resolveHooks.beforeScan = nil
		select {
		case <-release:
		default:
			close(release)
		}
	}()

	var wg sync.WaitGroup
	results := make([]dvid.UUID, 4)
	errs := make([]error, 4)
	wg.Add(4)
	for i := 0; i < 4; i++ {
		i := i
		go func() {
			defer wg.Done()
			if i == 0 {
				results[i], errs[i] = callResolve(t, env)
				return
			}
			// Give goroutine 0 a chance to reach the barrier.
			<-started
			results[i], errs[i] = callResolve(t, env)
		}()
	}
	<-started
	close(release)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent resolve %d failed: %v", i, err)
		}
	}
	for i := 1; i < 4; i++ {
		if results[i] != results[0] {
			t.Fatalf("concurrent resolve %d returned different child: %s vs %s", i, results[i], results[0])
		}
	}
	_, r, op := getResolveOp(t, env)
	if countResolveExtensions(r, op) != 1 || countMergeNodes(r) != 1 {
		t.Fatalf("concurrent resolves created extra nodes: extensions %d merges %d",
			countResolveExtensions(r, op), countMergeNodes(r))
	}
}

// TestResolveConcurrentDifferent: a different plan submitted while another
// resolve is active is rejected with ErrResolveBusy.
func TestResolveConcurrentDifferent(t *testing.T) {
	env := buildResolveTestEnv(t)
	defer CloseTest()

	started, release := holdResolveAtScan(t, env)
	defer func() {
		resolveHooks.beforeScan = nil
		select {
		case <-release:
		default:
			close(release)
		}
	}()

	done := make(chan struct{})
	go func() {
		_, _ = callResolve(t, env)
		close(done)
	}()
	<-started

	_, err := Resolve(env.root,
		[]dvid.InstanceName{"resdata1", "resdata2"},
		[]string{string(env.p2), string(env.p1)},
		"competing plan")
	if !errors.Is(err, ErrResolveBusy) {
		t.Fatalf("expected ErrResolveBusy, got %v", err)
	}

	close(release)
	<-done
}

// TestResolveConcurrentSaveRace: while a resolve is in flight, another
// client mutation on the same repo triggers gob encoding (r.save).  This
// must be race-free.  The unrelated branch also invalidates the frozen
// plan, so on release the runner reports stale.
func TestResolveConcurrentSaveRace(t *testing.T) {
	env := buildResolveTestEnv(t)
	defer CloseTest()

	started, release := holdResolveAtScan(t, env)
	defer func() {
		resolveHooks.beforeScan = nil
		select {
		case <-release:
		default:
			close(release)
		}
	}()

	done := make(chan error, 1)
	go func() {
		_, err := callResolve(t, env)
		done <- err
	}()
	<-started

	// Concurrent repo mutation: branch off a resolve parent, commit it
	// (r.save encodes the repo, including the running resolve's state).
	other, err := NewVersion(env.p1, "concurrent other work", "otherwork", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := Commit(other, "other work commit", nil); err != nil {
		t.Fatal(err)
	}

	close(release)
	if err := <-done; !errors.Is(err, ErrResolveStale) {
		t.Fatalf("expected stale after concurrent mutation, got %v", err)
	}
}

// TestResolveDistinctPlansConcurrent (F-4): many distinct plans submitted
// concurrently must produce exactly one runner; all others get ErrResolveBusy.
func TestResolveDistinctPlansConcurrent(t *testing.T) {
	env := buildResolveTestEnv(t)
	defer CloseTest()

	tt, err := TypeServiceByName("resolvetest")
	if err != nil {
		t.Fatal(err)
	}
	const n = 10
	for i := 0; i < n; i++ {
		name := dvid.InstanceName(fmt.Sprintf("solo%d", i))
		if _, err := NewData(env.root, tt, name, dvid.NewConfig()); err != nil {
			t.Fatal(err)
		}
	}

	barrier := make(chan struct{})
	var wg sync.WaitGroup
	var successCount, busyCount int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-barrier
			name := dvid.InstanceName(fmt.Sprintf("solo%d", i))
			_, err := Resolve(env.root,
				[]dvid.InstanceName{name},
				[]string{string(env.p1), string(env.p2)},
				"f4 distinct plan")
			switch {
			case err == nil:
				atomic.AddInt64(&successCount, 1)
			case errors.Is(err, ErrResolveBusy):
				atomic.AddInt64(&busyCount, 1)
			default:
				t.Errorf("unexpected error for plan %d: %v", i, err)
			}
		}(i)
	}
	close(barrier)
	wg.Wait()

	if successCount != 1 {
		t.Fatalf("expected exactly 1 successful runner, got %d (busy %d)", successCount, busyCount)
	}
	if busyCount != n-1 {
		t.Fatalf("expected %d busy rejections, got %d", n-1, busyCount)
	}

	// Exactly one batch op record exists in the map.
	_, r := getResolveRepo(t, env)
	var batchOps int
	for _, o := range r.resolveOps {
		if len(o.data) == 1 && strings.HasPrefix(string(o.data[0]), "solo") {
			batchOps++
		}
	}
	if batchOps != 1 {
		t.Fatalf("expected 1 inserted batch op, got %d", batchOps)
	}
}

// TestResolveConcurrentMergeRace (F-5): a standard Merge running while a
// resolve is paused exercises dag.nodes locking; -race must be clean.  The
// foreign merge child also makes the resolve stale on resume.
func TestResolveConcurrentMergeRace(t *testing.T) {
	env := buildResolveTestEnv(t)
	defer CloseTest()

	started, release := holdResolveAtScan(t, env)
	defer func() {
		resolveHooks.beforeScan = nil
		select {
		case <-release:
		default:
			close(release)
		}
	}()

	done := make(chan error, 1)
	go func() {
		_, err := callResolve(t, env)
		done <- err
	}()
	<-started

	// Standard conflict-free merge of the same parents, concurrent with resolve.
	_, err := Merge([]dvid.UUID{env.p1, env.p2}, "concurrent standard merge", MergeConflictFree)
	if err != nil {
		t.Fatal(err)
	}

	close(release)
	if err := <-done; !errors.Is(err, ErrResolveStale) {
		t.Fatalf("expected stale after concurrent merge, got %v", err)
	}
}

// TestResolveHideBranchRace (F-7): HideBranch deleting dag.nodes while
// duplicate resolve submissions (verifyFrozen) read them must be race-free.
func TestResolveHideBranchRace(t *testing.T) {
	env := buildResolveTestEnv(t)
	defer CloseTest()

	if _, err := callResolve(t, env); err != nil {
		t.Fatal(err)
	}

	const readers = 8
	barrier := make(chan struct{})
	var wg sync.WaitGroup

	// One hider.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-barrier
		if err := HideBranch(env.p2, "p2branch"); err != nil {
			t.Errorf("hide branch failed: %v", err)
		}
	}()

	// Concurrent duplicate submissions reading dag nodes.
	var resultCount int64
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-barrier
			_, err := callResolve(t, env)
			switch {
			case err == nil,
				errors.Is(err, ErrResolveStale),
				errors.Is(err, ErrResolveBadInput):
				atomic.AddInt64(&resultCount, 1)
			default:
				t.Errorf("unexpected resolve error: %v", err)
			}
		}()
	}

	close(barrier)
	wg.Wait()

	if resultCount != readers {
		t.Fatalf("only %d of %d readers completed", resultCount, readers)
	}

	// After the branch is hidden, the plan no longer resolves (stale or
	// bad input); the server must not report success.
	_, err := callResolve(t, env)
	if err == nil || (!errors.Is(err, ErrResolveStale) && !errors.Is(err, ErrResolveBadInput)) {
		t.Fatalf("expected stale/bad input after hide, got %v", err)
	}
}

// TestResolveHideDeleteDeadlock (F-8): HideBranch (global id/repo locks),
// DeleteRepo and repo listing running concurrently must not deadlock.
func TestResolveHideDeleteDeadlock(t *testing.T) {
	env := buildResolveTestEnv(t)
	defer CloseTest()

	// A second repo that will be deleted concurrently.
	otherRoot, err := NewRepo("delete target", "repo for f8", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := Commit(otherRoot, "delete target root", nil); err != nil {
		t.Fatal(err)
	}

	barrier := make(chan struct{})
	var wg sync.WaitGroup
	start := func(fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-barrier
			fn()
		}()
	}

	// Several hide-branch calls on the resolve repo.
	for i := 0; i < 4; i++ {
		start(func() {
			if err := HideBranch(env.p2, "p2branch"); err != nil &&
				!strings.Contains(err.Error(), "not found") {
				t.Errorf("hide branch: %v", err)
			}
		})
	}
	// Several repo deletions of independent throwaway repos.
	for i := 0; i < 4; i++ {
		root, err := NewRepo(fmt.Sprintf("throwaway %d", i), "f8", nil, "")
		if err != nil {
			t.Fatal(err)
		}
		start(func() {
			if err := DeleteRepo(root, ""); err != nil {
				t.Errorf("delete repo: %v", err)
			}
		})
	}
	// Several repo listings (id RLock -> repo RLock).
	for i := 0; i < 4; i++ {
		start(func() {
			m := getManager()
			if _, err := m.MarshalJSON(); err != nil {
				t.Errorf("repos list: %v", err)
			}
		})
	}

	// Watchdog: report a deadlock rather than hanging the test forever.
	finished := make(chan struct{})
	go func() {
		close(barrier)
		wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(20 * time.Second):
		t.Fatal("deadlock suspected: hide branch / delete repo / repos listing did not complete within 20s")
	}

	_ = otherRoot
}

// TestResolveDoneChildMissing: a final child that vanished makes duplicate
// submissions fail; the server must not fabricate a replacement node.
func TestResolveDoneChildMissing(t *testing.T) {
	env := buildResolveTestEnv(t)
	defer CloseTest()

	child, err := callResolve(t, env)
	if err != nil {
		t.Fatal(err)
	}

	m := getManager()
	childV, err := m.versionFromUUID(child)
	if err != nil {
		t.Fatal(err)
	}
	// Remove the child from id caches to simulate a missing final node.
	m.idMutex.Lock()
	delete(m.uuidToVersion, child)
	delete(m.versionToUUID, childV)
	m.idMutex.Unlock()

	_, err = callResolve(t, env)
	expectStale(t, err)
}
