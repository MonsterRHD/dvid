package datastore

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/storage"
)

// fakeKVStore is a minimal storage.KeyValueDB for pusher tests.
type fakeKVStore struct {
	mu   sync.Mutex
	data map[string][]byte

	arrived chan struct{} // signaled inside RawPut before the gate
	gate    chan struct{} // RawPut blocks until closed (nil = no blocking)
	failKey map[string]bool
}

func newFakeKVStore() *fakeKVStore {
	return &fakeKVStore{data: make(map[string][]byte)}
}

func (s *fakeKVStore) String() string                                     { return "fakeKVStore" }
func (s *fakeKVStore) Close()                                             {}
func (s *fakeKVStore) Equal(dvid.StoreConfig) bool                        { return false }
func (s *fakeKVStore) GetStoreConfig() dvid.StoreConfig                   { return dvid.StoreConfig{} }
func (s *fakeKVStore) Get(storage.Context, storage.TKey) ([]byte, error)  { return nil, nil }
func (s *fakeKVStore) Exists(storage.Context, storage.TKey) (bool, error) { return false, nil }
func (s *fakeKVStore) Put(storage.Context, storage.TKey, []byte) error    { return nil }
func (s *fakeKVStore) Delete(storage.Context, storage.TKey) error         { return nil }

func (s *fakeKVStore) RawPut(k storage.Key, v []byte) error {
	if s.arrived != nil {
		s.arrived <- struct{}{}
	}
	if s.gate != nil {
		<-s.gate
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failKey != nil && s.failKey[string(k)] {
		return errors.New("simulated RawPut failure")
	}
	s.data[string(k)] = v
	return nil
}

func (s *fakeKVStore) RawDelete(k storage.Key) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, string(k))
	return nil
}

func (s *fakeKVStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.data)
}

// dataKey builds a full data key.  Layout mirrors storage.constructDataKey:
// [dataKeyPrefix][instance 4][tkey][version 4][client 4][MarkData].
func dataKey(inst dvid.InstanceID, ver dvid.VersionID, tkey []byte) storage.Key {
	k := append([]byte{0x01}, inst.Bytes()...) // 0x01 == storage.dataKeyPrefix
	k = append(k, tkey...)
	k = append(k, ver.Bytes()...)
	k = append(k, dvid.ClientID(0).Bytes()...)
	return append(k, 0x03) // 0x03 == storage.MarkData
}

func newTestPusher() *pusher {
	p := &pusher{instances: make(map[uint32]*pushInstance)}
	if err := p.Open(1); err != nil {
		panic(err)
	}
	p.instanceMap = dvid.InstanceMap{10: 100}
	p.versionMap = dvid.VersionMap{20: 200}
	return p
}

func (p *pusher) addTestInstance(seq uint32, store storage.KeyValueDB) *pushInstance {
	inst := &pushInstance{
		name:  dvid.InstanceName(fmt.Sprintf("inst%d", seq)),
		stats: new(txStats),
		store: store,
	}
	p.mu.Lock()
	p.instances[seq] = inst
	p.mu.Unlock()
	return inst
}

func putKVMsg(seq uint32, n int) *KVMessage {
	return &KVMessage{
		Session: 1,
		Seq:     seq,
		KV: storage.KeyValue{
			K: dataKey(10, 20, []byte{byte(n >> 8), byte(n)}),
			V: []byte{byte(n)},
		},
	}
}

func terminateMsg(seq uint32) *KVMessage {
	return &KVMessage{Session: 1, Seq: seq, Terminate: true}
}

// Termination must block until all previously accepted puts committed, and
// every committed key must be remapped to local IDs.
func TestPusher_TerminateBarrierCommitsAllData(t *testing.T) {
	p := newTestPusher()
	store := newFakeKVStore()
	store.arrived = make(chan struct{}, 128)
	gate := make(chan struct{})
	store.gate = gate
	p.addTestInstance(1, store)

	const n = 100
	var wg sync.WaitGroup
	var failures int32
	wg.Add(n)
	for i := 1; i <= n; i++ {
		go func(i int) {
			defer wg.Done()
			if err := p.putData(putKVMsg(1, i)); err != nil {
				atomic.AddInt32(&failures, 1)
			}
		}(i)
	}
	for i := 0; i < n; i++ {
		<-store.arrived
	}

	termDone := make(chan error, 1)
	go func() { termDone <- p.putData(terminateMsg(1)) }()

	select {
	case err := <-termDone:
		t.Fatalf("terminate returned before puts committed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if c := store.count(); c != 0 {
		t.Fatalf("store should have no committed data behind gate, got %d", c)
	}

	close(gate)
	select {
	case err := <-termDone:
		if err != nil {
			t.Fatalf("terminate: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("terminate did not return after gate release")
	}
	wg.Wait()
	if atomic.LoadInt32(&failures) != 0 {
		t.Fatalf("%d puts failed", failures)
	}
	if c := store.count(); c != n {
		t.Fatalf("expected %d committed kvs, got %d", n, c)
	}

	// All stored keys carry the remapped local instance/version ids.
	for k := range store.data {
		inst, ver, _, err := storage.DataKeyToLocalIDs(storage.Key(k))
		if err != nil {
			t.Fatalf("stored key invalid: %v", err)
		}
		if inst != 100 || ver != 200 {
			t.Fatalf("stored key ids = (%d,%d), want (100,200)", inst, ver)
		}
	}

	// Duplicate termination is idempotent and still reports success.
	if err := p.putData(terminateMsg(1)); err != nil {
		t.Fatalf("duplicate terminate: %v", err)
	}
	if c := store.count(); c != n {
		t.Fatalf("duplicate terminate changed data: %d", c)
	}

	// Late data after termination gets a definitive rejection.
	if err := p.putData(putKVMsg(1, n+1)); !errors.Is(err, errDataAfterTerminate) {
		t.Fatalf("late put should be errDataAfterTerminate, got %v", err)
	}
	if c := store.count(); c != n {
		t.Fatalf("rejected late data was stored: %d", c)
	}
}

// An instance with no key-values terminates cleanly.
func TestPusher_EmptyInstanceTerminates(t *testing.T) {
	p := newTestPusher()
	store := newFakeKVStore()
	p.addTestInstance(7, store)

	if err := p.putData(terminateMsg(7)); err != nil {
		t.Fatalf("empty instance terminate: %v", err)
	}
	if err := p.putData(terminateMsg(7)); err != nil {
		t.Fatalf("duplicate empty instance terminate: %v", err)
	}
	if c := store.count(); c != 0 {
		t.Fatalf("empty instance stored %d keys", c)
	}
}

// A RawPut failure must surface on the terminating ack, and duplicate
// termination reports the same authoritative error.
func TestPusher_PutFailureSurfacesOnTerminate(t *testing.T) {
	p := newTestPusher()
	store := newFakeKVStore()
	bad := putKVMsg(2, 5)
	// commitKV remaps ids in place before RawPut, so fail on the post-remap key.
	store.failKey = map[string]bool{string(dataKey(100, 200, []byte{0, 5})): true}
	p.addTestInstance(2, store)

	good := putKVMsg(2, 1)
	if err := p.putData(good); err != nil {
		t.Fatalf("good put: %v", err)
	}
	if err := p.putData(bad); err == nil {
		t.Fatal("failing put should return error")
	}

	err := p.putData(terminateMsg(2))
	if err == nil {
		t.Fatal("terminate should surface the put failure")
	}
	err2 := p.putData(terminateMsg(2))
	if err2 == nil || err2.Error() != err.Error() {
		t.Fatalf("duplicate terminate should return same failure, got %v vs %v", err, err2)
	}
}

// Data for an instance that never started, or after close, is rejected.
func TestPusher_UnknownInstanceAndClose(t *testing.T) {
	p := newTestPusher()
	store := newFakeKVStore()
	p.addTestInstance(3, store)

	if err := p.putData(putKVMsg(99, 1)); err == nil {
		t.Fatal("put to unknown instance should fail")
	}
	if err := p.putData(terminateMsg(99)); err == nil {
		t.Fatal("terminate unknown instance should fail")
	}

	// Close without repo metadata is a successful no-op publish.
	if err := p.Close(); err != nil {
		t.Fatalf("close with nil repo: %v", err)
	}
	if err := p.putData(putKVMsg(3, 1)); err == nil {
		t.Fatal("put after close must fail")
	}
	if err := p.putData(terminateMsg(3)); err == nil {
		t.Fatal("terminate after close must fail")
	}
	// Idempotent close.
	if err := p.Close(); err != nil {
		t.Fatalf("duplicate close: %v", err)
	}
}

// Key values aren't accidentally shared between the incoming and stored key.
func TestPusher_KeyRemapPreservesTKey(t *testing.T) {
	p := newTestPusher()
	store := newFakeKVStore()
	p.addTestInstance(4, store)

	tkey := []byte{0x77, 0x01, 0x02}
	m := &KVMessage{
		Session: 1,
		Seq:     4,
		KV: storage.KeyValue{
			K: dataKey(10, 20, tkey),
			V: []byte("payload"),
		},
	}
	if err := p.putData(m); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := p.putData(terminateMsg(4)); err != nil {
		t.Fatalf("terminate: %v", err)
	}
	for k := range store.data {
		stored := storage.Key(k)
		// Instance id region remapped.
		if !bytes.Equal(stored[1:1+dvid.InstanceIDSize], dvid.InstanceID(100).Bytes()) {
			t.Fatal("instance id not remapped")
		}
		// TKey region preserved verbatim.
		if !bytes.Equal(stored[1+dvid.InstanceIDSize:1+dvid.InstanceIDSize+len(tkey)], tkey) {
			t.Fatal("tkey corrupted by remap")
		}
	}
}
