package rpc

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valyala/gorpc"
)

// ---- test doubles -------------------------------------------------------

const testPushMessage MessageID = "rpc.testPush"

type testMessage struct {
	Sid SessionID
	N   int
}

type testHandler struct {
	mu     sync.Mutex
	msgs   []int
	closed bool
	nClose int32
	fail   bool

	arrived chan struct{} // signaled on every deliver (optional gate)
	gate    chan struct{} // deliver blocks until this is closed (optional gate)
}

func (h *testHandler) ID() SessionID { return 0 }

func (h *testHandler) Open(id SessionID) error { return nil }

func (h *testHandler) Close() error {
	atomic.AddInt32(&h.nClose, 1)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	if h.fail {
		return errors.New("simulated close failure")
	}
	return nil
}

func (h *testHandler) deliver(n int) error {
	if h.arrived != nil {
		h.arrived <- struct{}{}
	}
	if h.gate != nil {
		<-h.gate
	}
	h.mu.Lock()
	h.msgs = append(h.msgs, n)
	h.mu.Unlock()
	return nil
}

func (h *testHandler) snapshot() (msgs []int, closed bool, nClose int32) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]int(nil), h.msgs...), h.closed, atomic.LoadInt32(&h.nClose)
}

func newTestHandler(MessageID) (SessionHandler, error) {
	return &testHandler{}, nil
}

var testRegisterOnce sync.Once

func registerTestInfrastructure() {
	testRegisterOnce.Do(func() {
		RegisterSessionMaker(testPushMessage, newTestHandler)
		d := Dispatcher()
		d.AddFunc("rpc.testDeliver", func(m *testMessage) error {
			return WithSessionHandler(m.Sid, func(h SessionHandler) error {
				return h.(*testHandler).deliver(m.N)
			})
		})
		gorpc.RegisterType(&testMessage{})
	})
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("can't grab free port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func startTestServer(t *testing.T, addr string) {
	t.Helper()
	registerTestInfrastructure()
	go func() {
		_ = StartServer(addr)
	}()
	waitListen(t, addr)
}

func waitListen(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.Dial("tcp", addr)
		if err == nil {
			c.Close()
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("rpc server at %s did not start", addr)
}

func newClientSession(t *testing.T, addr string, mid MessageID) *Session {
	t.Helper()
	var s Session
	var err error
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		s, err = NewSession(addr, mid)
		if err == nil {
			// Keep test failures fast instead of waiting out the 20s
			// production request timeout.
			s.c.RequestTimeout = 500 * time.Millisecond
			return &s
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("could not create session at %s: %v", addr, err)
	return nil
}

func deliver(t *testing.T, s *Session, n int) {
	t.Helper()
	if _, err := s.Call()("rpc.testDeliver", &testMessage{Sid: s.ID(), N: n}); err != nil {
		t.Fatalf("deliver %d failed: %v", n, err)
	}
}

func handlerFor(t *testing.T, sid SessionID) *testHandler {
	t.Helper()
	entry := lookupEntry(sid)
	if entry == nil {
		t.Fatalf("no session entry for %d", sid)
	}
	h, ok := entry.handler.(*testHandler)
	if !ok {
		t.Fatalf("unexpected handler type %T", entry.handler)
	}
	return h
}

// ---- server-side registry tests -----------------------------------------

// Data committed before end is visible to Close(); late data and duplicate
// end behave deterministically and idempotently.
func TestSession_EndBarrierAndIdempotentEnd(t *testing.T) {
	addr := freeAddr(t)
	startTestServer(t, addr)
	defer StopServer(addr)

	s := newClientSession(t, addr, testPushMessage)
	deliver(t, s, 1)
	deliver(t, s, 2)

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	h := handlerFor(t, s.ID())
	msgs, _, nClose := h.snapshot()
	if len(msgs) != 2 || msgs[0] != 1 || msgs[1] != 2 || nClose != 1 {
		t.Fatalf("expected committed [1 2] with one close, got %v (closes=%d)", msgs, nClose)
	}

	// Late data after close gets a definite, non-bad-session error.
	if err := WithSessionHandler(s.ID(), func(h SessionHandler) error {
		return h.(*testHandler).deliver(3)
	}); !errors.Is(err, ErrSessionEnded) {
		t.Fatalf("late data should return ErrSessionEnded, got %v", err)
	}

	// Duplicate end is idempotent and does not close again.
	if err := endSession(s.ID()); err != nil {
		t.Fatalf("duplicate end session: %v", err)
	}
	if got := atomic.LoadInt32(&h.nClose); got != 1 {
		t.Fatalf("Close ran %d times, expected exactly once", got)
	}

	// Truly unknown sessions remain bad sessions.
	if err := endSession(SessionID(1<<32 - 1)); !errors.Is(err, ErrBadSession) {
		t.Fatalf("unknown end session should be ErrBadSession, got %v", err)
	}
	if err := WithSessionHandler(SessionID(1<<32-1), func(SessionHandler) error { return nil }); !errors.Is(err, ErrBadSession) {
		t.Fatalf("unknown data session should be ErrBadSession, got %v", err)
	}
}

// endSession must wait for every accepted in-flight message to commit.
func TestSession_EndWaitsForInFlightData(t *testing.T) {
	addr := freeAddr(t)
	startTestServer(t, addr)
	defer StopServer(addr)

	const gatedMsg MessageID = "rpc.testPushGated"
	gate := make(chan struct{})
	arrived := make(chan struct{}, 64)
	RegisterSessionMaker(gatedMsg, func(MessageID) (SessionHandler, error) {
		return &testHandler{gate: gate, arrived: arrived}, nil
	})

	s := newClientSession(t, addr, gatedMsg)

	const n = 50
	var done int32
	for i := 0; i < n; i++ {
		go func(i int) {
			if _, err := s.Call()("rpc.testDeliver", &testMessage{Sid: s.ID(), N: i}); err != nil {
				t.Errorf("deliver %d: %v", i, err)
				return
			}
			atomic.AddInt32(&done, 1)
		}(i)
	}
	for i := 0; i < n; i++ {
		<-arrived
	}

	endReturned := make(chan error, 1)
	go func() { endReturned <- endSession(s.ID()) }()

	// While puts are parked, Close must not have run.
	select {
	case err := <-endReturned:
		t.Fatalf("endSession returned before data committed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	h := handlerFor(t, s.ID())
	if got := atomic.LoadInt32(&h.nClose); got != 0 {
		t.Fatalf("handler closed before barrier released")
	}

	close(gate)

	select {
	case err := <-endReturned:
		if err != nil {
			t.Fatalf("endSession: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("endSession did not return after barrier release")
	}

	if atomic.LoadInt32(&done) != n {
		// Handlers have finished; the RPC responses themselves may still
		// be traversing the wire.  Give them a moment.
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) && atomic.LoadInt32(&done) != n {
			time.Sleep(5 * time.Millisecond)
		}
	}
	if atomic.LoadInt32(&done) != n {
		t.Fatalf("only %d/%d data messages acknowledged", done, n)
	}
	msgs, _, nClose := h.snapshot()
	if len(msgs) != n || nClose != 1 {
		t.Fatalf("expected %d committed msgs and 1 close, got %d msgs %d closes", n, len(msgs), nClose)
	}
}

// Abort never publishes; post-abort data is rejected; duplicate abort is
// idempotent; ending an aborted session fails deterministically.
func TestSession_AbortSemantics(t *testing.T) {
	addr := freeAddr(t)
	startTestServer(t, addr)
	defer StopServer(addr)

	s := newClientSession(t, addr, testPushMessage)
	deliver(t, s, 7)

	if err := s.Abort(); err != nil {
		t.Fatalf("abort: %v", err)
	}
	h := handlerFor(t, s.ID())
	if _, closed, _ := h.snapshot(); closed {
		t.Fatal("aborted session must not run handler Close")
	}
	if err := abortSession(s.ID()); err != nil {
		t.Fatalf("duplicate abort: %v", err)
	}
	if err := WithSessionHandler(s.ID(), func(h SessionHandler) error { return nil }); !errors.Is(err, ErrSessionAborted) {
		t.Fatalf("data after abort should be ErrSessionAborted, got %v", err)
	}
	if err := endSession(s.ID()); !errors.Is(err, ErrSessionAborted) {
		t.Fatalf("end after abort should report ErrSessionAborted, got %v", err)
	}
	if got := atomic.LoadInt32(&h.nClose); got != 0 {
		t.Fatalf("aborted handler was closed %d times", got)
	}
}

// Concurrent end/abort must reach exactly one terminal state without races
// or panics, repeatedly.
func TestSession_ConcurrentEndAbort(t *testing.T) {
	addr := freeAddr(t)
	startTestServer(t, addr)
	defer StopServer(addr)

	for iter := 0; iter < 20; iter++ {
		s := newClientSession(t, addr, testPushMessage)
		for i := 0; i < 10; i++ {
			go func(i int) {
				_ = WithSessionHandler(s.ID(), func(h SessionHandler) error {
					return h.(*testHandler).deliver(i)
				})
			}(i)
		}
		var endOK, abortOK int32
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			if endSession(s.ID()) == nil {
				atomic.AddInt32(&endOK, 1)
			}
		}()
		go func() {
			defer wg.Done()
			if abortSession(s.ID()) == nil {
				atomic.AddInt32(&abortOK, 1)
			}
		}()
		wg.Wait()

		h := handlerFor(t, s.ID())
		nClose := atomic.LoadInt32(&h.nClose)
		switch {
		case nClose > 1:
			t.Fatalf("iter %d: close ran %d times", iter, nClose)
		case nClose == 1 && atomic.LoadInt32(&abortOK) != 0:
			t.Fatalf("iter %d: both end and abort reported success", iter)
		case nClose == 0 && atomic.LoadInt32(&endOK) != 0:
			t.Fatalf("iter %d: end reported success without close", iter)
		}

		err := WithSessionHandler(s.ID(), func(h SessionHandler) error { return nil })
		if !errors.Is(err, ErrSessionEnded) && !errors.Is(err, ErrSessionAborted) {
			t.Fatalf("iter %d: late data got %v, want terminal error", iter, err)
		}
		s.c.Stop() // don't leak client connections across iterations
	}
}

// ---- client behavior tests ----------------------------------------------

func TestClient_CloseConfirmedAndIdempotent(t *testing.T) {
	addr := freeAddr(t)
	startTestServer(t, addr)
	defer StopServer(addr)

	s := newClientSession(t, addr, testPushMessage)
	deliver(t, s, 1)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close should be idempotent, got %v", err)
	}
	if s.Aborted() {
		t.Fatal("closed session should not report aborted")
	}
	h := handlerFor(t, s.ID())
	if _, closed, nClose := h.snapshot(); !closed || nClose != 1 {
		t.Fatalf("server state wrong: closed=%v closes=%d", closed, nClose)
	}
}

func TestClient_CloseUnconfirmedOnServerDown(t *testing.T) {
	addr := freeAddr(t)
	startTestServer(t, addr)

	s := newClientSession(t, addr, testPushMessage)
	deliver(t, s, 1)

	// Drop the server before termination so the end cannot be confirmed.
	if err := StopServer(addr); err != nil {
		t.Fatal(err)
	}

	// Keep retries fast.
	oldBackoff := endBackoff
	endBackoff = []time.Duration{time.Millisecond, 2 * time.Millisecond}
	defer func() { endBackoff = oldBackoff }()

	start := time.Now()
	err := s.Close()
	if err == nil {
		t.Fatal("expected unconfirmed close error, got nil")
	}
	if !errors.Is(err, ErrSessionEndUnconfirmed) {
		t.Fatalf("expected ErrSessionEndUnconfirmed, got %v", err)
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("close retries took too long: %v", d)
	}
}

func TestClient_AbortAfterFailure(t *testing.T) {
	addr := freeAddr(t)
	startTestServer(t, addr)
	defer StopServer(addr)

	s := newClientSession(t, addr, testPushMessage)
	deliver(t, s, 1)
	if err := s.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if !s.Aborted() {
		t.Fatal("Aborted() should be true")
	}
	if err := s.Abort(); err != nil {
		t.Fatalf("duplicate Abort should be idempotent, got %v", err)
	}
	h := handlerFor(t, s.ID())
	if _, closed, _ := h.snapshot(); closed {
		t.Fatal("aborted session must not be closed on the server")
	}
}

// Close must wait for jobs registered via StartJob/StopJob before asking the
// server to end the session.
func TestClient_CloseWaitsForJobs(t *testing.T) {
	addr := freeAddr(t)
	startTestServer(t, addr)
	defer StopServer(addr)

	s := newClientSession(t, addr, testPushMessage)
	s.StartJob()
	ended := make(chan struct{})
	go func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
		close(ended)
	}()
	select {
	case <-ended:
		t.Fatal("Close returned before StopJob")
	case <-time.After(100 * time.Millisecond):
	}
	s.StopJob()
	select {
	case <-ended:
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not return after StopJob")
	}
}
