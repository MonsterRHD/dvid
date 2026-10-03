package datastore

import (
	"encoding/json"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

// R5 temporary probe: HideBranch's global phase is idMutex -> repoMutex;
// pre-existing LimitVersions is repoMutex -> idMutex.  Look for a natural
// ABBA across many rounds with small random jitter.
func TestR5HideLimitDeadlock(t *testing.T) {
	env := buildResolveTestEnv(t)
	m := getManager()

	target := env.p2
	for round := 0; round < 200; round++ {
		// Config listing EVERY m.repos key so LimitVersions mutates nothing
		// (it still acquires repoMutex -> idMutex unconditionally).
		m.repoMutex.RLock()
		uuids := make([]string, 0, len(m.repos))
		for u := range m.repos {
			uuids = append(uuids, string(u))
		}
		m.repoMutex.RUnlock()
		cfgBytes, _ := json.Marshal(struct {
			Versions []string
		}{uuids})
		cfg := "/tmp/r5limit_config.json"
		if err := os.WriteFile(cfg, cfgBytes, 0644); err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		barrier := make(chan struct{})

		for k := 0; k < 2; k++ {
			wg.Add(1)
			go func() { // G1: HideBranch (idMutex -> repoMutex at global phase)
				defer wg.Done()
				<-barrier
				time.Sleep(time.Duration(rand.Intn(5000)) * time.Microsecond)
				if err := HideBranch(target, "p2branch"); err != nil {
					t.Logf("round %d hide: %v", round, err)
				}
			}()
			wg.Add(1)
			go func() { // G2: LimitVersions (repoMutex -> idMutex)
				defer wg.Done()
				<-barrier
				time.Sleep(time.Duration(rand.Intn(5000)) * time.Microsecond)
				if err := LimitVersions(env.root, cfg); err != nil {
					t.Logf("round %d limit: %v", round, err)
				}
			}()
		}

		close(barrier)
		finished := make(chan struct{})
		go func() {
			wg.Wait()
			close(finished)
		}()
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Logf("ROUND %d STACK DUMP:\n%s", round, buf[:n])
			t.Fatalf("DEADLOCK CONFIRMED at round %d", round)
		}

		// Recreate the hidden branch and track its new tip.
		child, err := NewVersion(env.root, "recreate p2branch", "p2branch", nil)
		if err != nil {
			t.Logf("round %d recreate: %v", round, err)
			continue
		}
		if err := Commit(child, "p2 commit", nil); err != nil {
			t.Logf("round %d commit: %v", round, err)
		}
		target = child
	}

	os.Remove("/tmp/r5limit_config.json")
}

// Second R5 probe: hide local phase r.Lock -> dag.Lock -> node.Lock vs
// pre-existing SetNodeNote node.Lock -> r.Lock.
func TestR5HideNoteDeadlock(t *testing.T) {
	env := buildResolveTestEnv(t)

	target := env.p2
	for round := 0; round < 200; round++ {
		var wg sync.WaitGroup
		barrier := make(chan struct{})
		for k := 0; k < 2; k++ {
			wg.Add(1)
			go func() { // hider: r -> dag -> node
				defer wg.Done()
				<-barrier
				time.Sleep(time.Duration(rand.Intn(5000)) * time.Microsecond)
				if err := HideBranch(target, "p2branch"); err != nil {
					t.Logf("round %d hide: %v", round, err)
				}
			}()
			wg.Add(1)
			go func() { // note setter on root: node -> r
				defer wg.Done()
				<-barrier
				time.Sleep(time.Duration(rand.Intn(5000)) * time.Microsecond)
				if err := SetNodeNote(env.root, "r5 note"); err != nil {
					t.Logf("round %d note: %v", round, err)
				}
			}()
		}
		close(barrier)
		finished := make(chan struct{})
		go func() {
			wg.Wait()
			close(finished)
		}()
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Logf("ROUND %d STACK DUMP:\n%s", round, buf[:n])
			t.Fatalf("DEADLOCK CONFIRMED at round %d", round)
		}
		child, err := NewVersion(env.root, "recreate p2branch", "p2branch", nil)
		if err != nil {
			t.Logf("round %d recreate: %v", round, err)
			continue
		}
		_ = Commit(child, "p2 commit", nil)
		target = child
	}
}

