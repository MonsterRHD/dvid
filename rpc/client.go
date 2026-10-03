/*
	This file implements high-throughput messaging using gorpc.
*/

package rpc

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/valyala/gorpc"
)

// MessageID should be a unique string across the DVID code base.
// By convention, the package name should be prefixed to the type
// of session being performed, e.g., "datastore.Push"
type MessageID string

// Caller is a function that meets the rpc Call signature.
type Caller func(string, interface{}) (interface{}, error)

// ErrSessionEndUnconfirmed wraps the failure of Session.Close when the
// remote server could not durably acknowledge session termination.  The
// push may or may not have completed remotely, so the whole push can be
// retried safely.
var ErrSessionEndUnconfirmed = errors.New("session end was not confirmed by remote server; outcome unknown and push is safe to retry")

// endAttempts is the number of attempts used to confirm termination, and
// endBackoff the delays between attempts.  gorpc re-dials on its own ~1s
// reconnect loop, so the attempts cover transient connection loss.
const (
	endAttempts = 4
)

var endBackoff = []time.Duration{250 * time.Millisecond, 500 * time.Millisecond, time.Second}

// sessionTerm holds the one-shot termination state of a Session.  It is
// referenced by pointer because Session values are copied when returned from
// NewSession and when embedded into PushSession.
type sessionTerm struct {
	once    sync.Once
	mu      sync.Mutex
	aborted bool
	err     error
}

// Session provides ability to send data to remote DVID using multiple
// RPCs.
type Session struct {
	sync.WaitGroup
	c  *gorpc.Client
	dc *gorpc.DispatcherClient
	id SessionID

	term *sessionTerm
}

// NewSession returns a new session to the remote address where the
// type of session is reflected by the MessageID.
func NewSession(addr string, mid MessageID) (Session, error) {
	c := gorpc.NewTCPClient(addr)
	c.Start()
	dc := dispatcher.NewFuncClient(c)
	if dc == nil {
		return Session{}, fmt.Errorf("can't create dispatcher client")
	}

	// Start session create RPC using the given message, which
	// should remotely use the registered session maker and return
	// a session ID.
	resp, err := dc.Call(sendNewSession, mid)
	if err != nil {
		c.Stop()
		return Session{}, err
	}
	sid, ok := resp.(SessionID)
	if !ok {
		c.Stop()
		return Session{}, fmt.Errorf("remote server returned %v instead of session id", resp)
	}

	// Create the session.
	return Session{c: c, dc: dc, id: sid, term: &sessionTerm{}}, nil
}

func (s *Session) ID() SessionID {
	return s.id
}

func (s *Session) Call() Caller {
	return s.dc.Call
}

// StartJob marks the start of a sequence of calls that may
// be handled concurrently.  A session will wait for StopJob()
// before allowing proceeding with any closing of the session.
func (s *Session) StartJob() {
	s.Add(1)
}

func (s *Session) StopJob() {
	s.Done()
}

// Close gracefully terminates the session: it waits for all outstanding
// jobs, asks the server to commit the session, and blocks until the server
// acknowledges that every previously accepted message has committed.
// Repeated calls are idempotent and return the result of the first Close.
// Connection failures are retried (gorpc reconnects underneath); if the
// server cannot confirm termination, an error wrapping
// ErrSessionEndUnconfirmed is returned so the caller can safely retry the
// whole transfer.
func (s *Session) Close() error {
	s.term.once.Do(func() {
		s.term.err = s.finish(false)
	})
	return s.term.err
}

// Abort discards the session: pending jobs are drained, the server is told
// not to publish any session results, and the client is stopped.  It is
// best-effort and idempotent; calling Abort after Close returns the Close
// result.
func (s *Session) Abort() error {
	s.term.once.Do(func() {
		s.term.mu.Lock()
		s.term.aborted = true
		s.term.mu.Unlock()
		s.term.err = s.finish(true)
	})
	return s.term.err
}

// Aborted reports whether Abort was used to terminate this session.
func (s *Session) Aborted() bool {
	s.term.mu.Lock()
	defer s.term.mu.Unlock()
	return s.term.aborted
}

func (s *Session) finish(abort bool) error {
	dvid.Debugf("session %d close: waiting for any jobs to complete...\n", s.id)
	s.Wait()

	var err error
	if abort {
		dvid.Debugf("sending session abort to remote...\n")
		// Abort is best effort: a dropped connection means the server will
		// simply never publish the session.
		_, err = s.dc.Call(sendAbortSession, s.id)
	} else {
		dvid.Debugf("sending session end to remote...\n")
		err = s.confirmEnd()
	}

	dvid.Debugf("stopping client...\n")
	s.c.Stop()
	return err
}

// confirmEnd calls EndSession until the server commits the session or a
// definitive server error is returned.  Connection/timeout errors are
// retried because gorpc re-establishes the TCP connection automatically;
// the server's endSession is idempotent, so a delayed retry cannot
// double-commit.  A missing session on retry (ErrBadSession) means server
// state was lost and the outcome is unconfirmed.
func (s *Session) confirmEnd() error {
	var lastErr error
	for attempt := 0; attempt < endAttempts; attempt++ {
		if attempt > 0 {
			idx := attempt - 1
			if idx >= len(endBackoff) {
				idx = len(endBackoff) - 1
			}
			time.Sleep(endBackoff[idx])
		}
		_, err := s.dc.Call(sendEndSession, s.id)
		if err == nil {
			return nil
		}
		if isRetriableCallErr(err) {
			dvid.Infof("session %d end attempt %d failed with connection error, will retry: %v\n", s.id, attempt+1, err)
			lastErr = err
			continue
		}
		// Definitive server-side error (bad session, failed commit, ...).
		return err
	}
	return fmt.Errorf("%w (session %d): %v", ErrSessionEndUnconfirmed, s.id, lastErr)
}

// isRetriableCallErr reports whether an RPC error may disappear on
// reconnect/retry and cannot itself prove that the server rejected the
// session.
func isRetriableCallErr(err error) bool {
	var ce *gorpc.ClientError
	if errors.As(err, &ce) {
		return ce.Connection || ce.Timeout || ce.Canceled
	}
	// DispatcherClient wraps all errors in *gorpc.ClientError, so anything
	// else is treated as a definitive, non-retriable error.
	return false
}
