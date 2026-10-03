//go:build !clustered && !gcloud
// +build !clustered,!gcloud

package datastore

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/janelia-flyem/dvid/dvid"
)

// TestResolveCommitFailure covers commit-phase failure with direct retry
// and process-restart retry: each extension is committed exactly once and
// the final merge child remains unique.
func TestResolveCommitFailure(t *testing.T) {
	for _, restart := range []bool{false, true} {
		name := "direct"
		if restart {
			name = "restart"
		}
		t.Run(name, func(t *testing.T) {
			env := buildResolveTestEnv(t)
			defer CloseTest()

			var failed int32
			resolveHooks.beforeCommit = func(extUUID dvid.UUID) error {
				if atomic.CompareAndSwapInt32(&failed, 0, 1) {
					return errors.New("injected commit failure")
				}
				return nil
			}
			defer func() { resolveHooks.beforeCommit = nil }()

			_, err := callResolve(t, env)
			if err == nil {
				t.Fatal("expected commit failure error")
			}
			m, r, op := getResolveOp(t, env)
			if op.phase != phaseCommitting {
				t.Fatalf("expected phase committing, got %d", op.phase)
			}
			if op.extCommitted[1] {
				t.Fatal("extension marked committed after failed commit")
			}
			extV, err := m.versionFromUUID(op.extensions[1])
			if err != nil {
				t.Fatal(err)
			}
			if r.dag.nodes[extV].locked {
				t.Fatal("extension locked after failed commit")
			}
			if countMergeNodes(r) != 0 {
				t.Fatal("merge node created before successful commit")
			}

			if restart {
				CloseReopenTest()
			}

			child, err := callResolve(t, env)
			if err != nil {
				t.Fatalf("resolve failed after %s retry: %v", name, err)
			}
			_, r2, op2 := getResolveOp(t, env)
			if op2.phase != phaseDone {
				t.Fatalf("expected done, got %d", op2.phase)
			}
			if !op2.extCommitted[1] {
				t.Fatal("extension not committed on retry")
			}
			if countResolveExtensions(r2, op2) != 1 || countMergeNodes(r2) != 1 {
				t.Fatalf("unexpected node counts: extensions %d merges %d",
					countResolveExtensions(r2, op2), countMergeNodes(r2))
			}
			if child != op2.child {
				t.Fatal("returned child differs from record")
			}
		})
	}
}

// mergeFailureState prepares an op paused inside the merge phase at one of
// the three crash windows and returns the frozen child UUID.
type mergeWindow int

const (
	// window 1: child reserved & persisted, not registered in id caches
	mergeWindowReserved mergeWindow = iota
	// window 2: child registered in caches, absent from DAG
	mergeWindowCached
	// window 3: merge DAG node built, op not marked done
	mergeWindowBuilt
)

func pauseAtMergeWindow(t *testing.T, env *resolveTestEnv, w mergeWindow) dvid.UUID {
	t.Helper()
	var fired int32
	resolveHooks.beforeMerge = func(child dvid.UUID) error {
		if !atomic.CompareAndSwapInt32(&fired, 0, 1) {
			return nil
		}
		m := getManager()
		switch w {
		case mergeWindowReserved:
			// Nothing: hook runs after child reservation, before registration.
		case mergeWindowCached:
			if _, _, err := m.newUUID(&child); err != nil {
				t.Fatal(err)
			}
		case mergeWindowBuilt:
			_, _, op := getResolveOp(t, env)
			if err := m.mergeAssigned(op.mergeParents(), child, op.note); err != nil {
				t.Fatal(err)
			}
		}
		return errors.New("injected merge pause")
	}
	defer func() { resolveHooks.beforeMerge = nil }()

	_, err := callResolve(t, env)
	if err == nil {
		t.Fatal("expected merge-phase pause error")
	}
	_, _, op := getResolveOp(t, env)
	if op.phase != phaseMerging {
		t.Fatalf("expected phase merging, got %d", op.phase)
	}
	return op.child
}

// TestResolveMergeCrashWindows verifies resume (direct & after restart) from
// each merge crash window: same child, single merge node, no duplicate wiring.
func TestResolveMergeCrashWindows(t *testing.T) {
	windows := []struct {
		name string
		w    mergeWindow
	}{
		{"reserved-unregistered", mergeWindowReserved},
		{"cached-no-dag", mergeWindowCached},
		{"dag-built-not-done", mergeWindowBuilt},
	}
	for _, tw := range windows {
		for _, restart := range []bool{false, true} {
			caseName := tw.name
			if restart {
				caseName += "/restart"
			}
			t.Run(caseName, func(t *testing.T) {
				env := buildResolveTestEnv(t)
				defer CloseTest()

				child := pauseAtMergeWindow(t, env, tw.w)
				_, r, op := getResolveOp(t, env)
				if countResolveExtensions(r, op) != 1 {
					t.Fatal("expected one extension")
				}

				if restart {
					CloseReopenTest()
				}

				got, err := callResolve(t, env)
				if err != nil {
					t.Fatalf("resume from %s failed: %v", caseName, err)
				}
				if got != child {
					t.Fatalf("resume created different child: %s vs %s", got, child)
				}
				_, r2, op2 := getResolveOp(t, env)
				if op2.phase != phaseDone {
					t.Fatalf("expected done, got %d", op2.phase)
				}
				if countMergeNodes(r2) != 1 {
					t.Fatalf("expected single merge node after resume, got %d", countMergeNodes(r2))
				}
				if countResolveExtensions(r2, op2) != 1 {
					t.Fatal("resume duplicated extensions")
				}

				// Merge node must carry exactly the expected two parents.
				m := getManager()
				childV, err := m.versionFromUUID(child)
				if err != nil {
					t.Fatal(err)
				}
				extV, err := m.versionFromUUID(op2.extensions[1])
				if err != nil {
					t.Fatal(err)
				}
				if !sameVersionSet(r2.dag.nodes[childV].parents, []dvid.VersionID{env.p1V, extV}) {
					t.Fatalf("merge parents changed after resume: %v", r2.dag.nodes[childV].parents)
				}

				// Values at the resumed child follow priority.
				d1, err := GetDataByUUIDName(env.root, "resdata1")
				if err != nil {
					t.Fatal(err)
				}
				d2, err := GetDataByUUIDName(env.root, "resdata2")
				if err != nil {
					t.Fatal(err)
				}
				env.d1, env.d2 = d1, d2
				assertChildReads(t, env, child)
			})
		}
	}
}
