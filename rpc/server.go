/*
	This file implements high-throughput messaging using gorpc.
*/

package rpc

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/valyala/gorpc"
)

const (
	// The default address for internal messaging to this DVID server
	DefaultAddress = "localhost:8002"
)

var (
	// dispatcher provides wrapper to route calls.
	dispatcher *gorpc.Dispatcher

	// servers assigned to different ports
	servers map[string]*gorpc.Server

	// keep track of sessions so incoming messages are routed to
	// appropriate session.
	handlers map[SessionID]*sessionEntry

	// new session ID
	sessionID SessionID

	// when a given message is received from client, create new session
	// using functions provided by various packages.
	registered map[MessageID]NewSessionHandlerFunc

	registeredMu, handlersMu sync.RWMutex
)

var (
	ErrNoServerRunning     = errors.New("no servers are running")
	ErrServerNotFound      = errors.New("server not found")
	ErrClientUninitialized = errors.New("client not initialized")
	ErrBadSession          = errors.New("bad session id; not found on server")

	// ErrSessionEnding is returned for session data that arrives while the
	// session is being terminated.  The data was not processed and can be
	// retried safely on a new session.
	ErrSessionEnding = errors.New("session is ending; data message rejected")

	// ErrSessionEnded is returned for session data arriving after the
	// session has terminated.  The data was definitely not processed.
	ErrSessionEnded = errors.New("session has ended; data message arrived too late")

	// ErrSessionAborted is returned for session data arriving after the
	// client aborted the session, or when termination is attempted on an
	// aborted session.
	ErrSessionAborted = errors.New("session was aborted before this message")
)

// terminalSessionTTL is how long a terminated or aborted session is kept
// around so duplicate end/abort calls and in-flight late messages get
// deterministic results instead of a generic bad-session error.
const terminalSessionTTL = 5 * time.Minute

var (
	sendNewSession   = "NewSession"
	sendEndSession   = "EndSession"
	sendAbortSession = "AbortSession"
)

func init() {
	registered = make(map[MessageID]NewSessionHandlerFunc)
	handlers = make(map[SessionID]*sessionEntry)

	// Send gorpc errors to DVID's error log.  gorpc stores this in a
	// package-global read while clients start, so set it once here rather
	// than on every StartServer to avoid data races.
	gorpc.SetErrorLogger(dvid.Errorf)

	// Create dispatcher for session creation and other functions.
	d := gorpc.NewDispatcher()
	d.AddFunc(sendNewSession, newSession)
	d.AddFunc(sendEndSession, endSession)
	d.AddFunc(sendAbortSession, abortSession)
	dispatcher = d

	var m MessageID
	gorpc.RegisterType(m)
	var s SessionID
	gorpc.RegisterType(s)
}

func Dispatcher() *gorpc.Dispatcher {
	return dispatcher
}

// Transmit describes how data is being transferred from DVID-to-DVID.
type Transmit uint8

const (
	TransmitUnknown Transmit = iota
	TransmitAll              // send all versions to maintain the full DAG
	TransmitBranch           // send versions in ancestor path that are not in local DAG
	TransmitFlatten          // send just the version required and do required copy from earlier versions
)

// SessionID uniquely identifies a rpc session.
type SessionID uint64

type MessageHandler func(interface{}) (interface{}, error)

// SessionHandler has a state, processes incoming messages via a finite state
// machine, and knows how to terminate itself upon receiving a termination
// message, calling the rpc package EndSession.
type SessionHandler interface {
	ID() SessionID
	Open(SessionID) error
	Close() error
}

// NewSessionHandlerFunc is a function that creates new session handlers
type NewSessionHandlerFunc func(MessageID) (SessionHandler, error)

// RegisterSessionMaker allows any package to register a function to create
// a new session handler given a designated message type.
func RegisterSessionMaker(id MessageID, f NewSessionHandlerFunc) {
	registeredMu.Lock()
	registered[id] = f
	registeredMu.Unlock()
}

// sessionState tracks the lifecycle of a single session.
type sessionState uint8

const (
	sessionActive  sessionState = iota // accepting messages
	sessionClosing                     // end requested; draining in-flight messages
	sessionClosed                      // handler Close() has run
	sessionAborted                     // client gave up; Close() must not run
)

// sessionEntry wraps a SessionHandler with the synchronization needed to make
// termination deterministic despite gorpc dispatching every incoming request
// to its own goroutine.
type sessionEntry struct {
	handler SessionHandler

	// mu guards state and result.  inFlight counts messages that have been
	// accepted while the session was active.
	mu       sync.Mutex
	state    sessionState
	result   error
	inFlight sync.WaitGroup

	// done is closed once the entry reaches a terminal state (closed or
	// aborted).  Duplicate end/abort calls block on this so they observe the
	// authoritative result.
	done chan struct{}
}

// accept reserves an in-flight slot for a session message.  It returns a
// release function on success.  Messages arriving while the session is no
// longer active receive a deterministic error.
func (e *sessionEntry) accept() (func(), error) {
	e.mu.Lock()
	switch e.state {
	case sessionActive:
		e.inFlight.Add(1)
		e.mu.Unlock()
		return e.inFlight.Done, nil
	case sessionClosing:
		e.mu.Unlock()
		return nil, ErrSessionEnding
	case sessionClosed:
		e.mu.Unlock()
		return nil, ErrSessionEnded
	default:
		e.mu.Unlock()
		return nil, ErrSessionAborted
	}
}

func (e *sessionEntry) terminalError() error {
	state, result := e.awaitTerminal()
	switch state {
	case sessionAborted:
		return ErrSessionAborted
	default:
		return result
	}
}

// awaitTerminal blocks until the entry is terminal and returns its state.
func (e *sessionEntry) awaitTerminal() (sessionState, error) {
	<-e.done
	e.mu.Lock()
	state, result := e.state, e.result
	e.mu.Unlock()
	return state, result
}

func newSession(msgID MessageID) (SessionID, error) {
	registeredMu.RLock()
	sessionMaker, found := registered[msgID]
	registeredMu.RUnlock()
	if !found {
		return 0, fmt.Errorf("unknown message ID for session: %q", msgID)
	}
	if sessionMaker == nil {
		return 0, fmt.Errorf("no session maker found for %q", msgID)
	}

	handler, err := sessionMaker(msgID)
	if err != nil {
		return 0, err
	}

	handlersMu.Lock()
	sessionID++
	id := sessionID
	entry := &sessionEntry{
		handler: handler,
		state:   sessionActive,
		done:    make(chan struct{}),
	}
	handlers[id] = entry
	handlersMu.Unlock()

	if err := handler.Open(id); err != nil {
		// Don't leak a half-opened handler.  Remove it directly; it never
		// reached a terminal state so no reaper is scheduled.
		handlersMu.Lock()
		if cur, ok := handlers[id]; ok && cur == entry {
			delete(handlers, id)
		}
		handlersMu.Unlock()
		return 0, err
	}

	return id, nil
}

// lookupEntry returns the session entry or nil.
func lookupEntry(id SessionID) *sessionEntry {
	handlersMu.RLock()
	entry := handlers[id]
	handlersMu.RUnlock()
	return entry
}

// finish drains in-flight messages, runs the handler's Close() exactly once,
// and retains the authoritative result for duplicate calls.  It must be
// called outside of WithSessionHandler; otherwise the WaitGroup wait would
// deadlock on the calling message itself.
func endSession(id SessionID) error {
	entry := lookupEntry(id)
	if entry == nil {
		return ErrBadSession
	}

	entry.mu.Lock()
	switch entry.state {
	case sessionClosing, sessionClosed:
		entry.mu.Unlock()
		state, result := entry.awaitTerminal()
		if state == sessionAborted {
			return ErrSessionAborted
		}
		return result
	case sessionAborted:
		entry.mu.Unlock()
		return entry.terminalError()
	default: // sessionActive
		entry.state = sessionClosing
		entry.mu.Unlock()
	}

	// Wait for every message accepted before the state flip to commit.  No
	// new messages can be accepted once the state is sessionClosing.
	entry.inFlight.Wait()

	err := entry.handler.Close()

	entry.mu.Lock()
	entry.state = sessionClosed
	entry.result = err
	close(entry.done)
	entry.mu.Unlock()

	scheduleSessionReap(id, entry)
	return err
}

// abortSession discards a session without publishing its results.  It is
// idempotent and never calls the handler's Close().
func abortSession(id SessionID) error {
	entry := lookupEntry(id)
	if entry == nil {
		return ErrBadSession
	}

	entry.mu.Lock()
	switch entry.state {
	case sessionAborted:
		entry.mu.Unlock()
		_, _ = entry.awaitTerminal()
		return nil // idempotent duplicate abort
	case sessionClosing, sessionClosed:
		// End is in flight or already committed; abort cannot undo it.
		entry.mu.Unlock()
		state, result := entry.awaitTerminal()
		if state == sessionAborted {
			return nil
		}
		if result != nil {
			return result
		}
		return ErrSessionEnded
	default:
		entry.state = sessionClosing
		entry.mu.Unlock()
	}

	entry.inFlight.Wait()

	entry.mu.Lock()
	entry.state = sessionAborted
	close(entry.done)
	entry.mu.Unlock()

	scheduleSessionReap(id, entry)
	return nil
}

// scheduleSessionReap removes the terminal session entry after a grace
// period during which duplicate control messages and stragglers still get
// deterministic results.
func scheduleSessionReap(id SessionID, entry *sessionEntry) {
	time.AfterFunc(terminalSessionTTL, func() {
		handlersMu.Lock()
		if cur, ok := handlers[id]; ok && cur == entry {
			delete(handlers, id)
		}
		handlersMu.Unlock()
	})
}

// WithSessionHandler routes a data message to the handler identified by sid,
// enforcing the session lifecycle: messages are rejected once termination or
// abort has begun, and every accepted message is counted as in-flight so the
// terminator can wait for its commit.  The supplied function is called with
// the live handler while the message is counted in-flight.
func WithSessionHandler(sid SessionID, fn func(SessionHandler) error) error {
	entry := lookupEntry(sid)
	if entry == nil {
		return ErrBadSession
	}
	release, err := entry.accept()
	if err != nil {
		return err
	}
	defer release()
	return fn(entry.handler)
}

// StartServer starts an RPC server.
func StartServer(address string) error {
	defer func() {
		if e := recover(); e != nil {
			msg := fmt.Sprintf("Panic detected on rpc serve thread: %+v\n", e)
			hostname, _ := os.Hostname()
			dvid.ReportPanic(msg, hostname)
		}
	}()

	s := gorpc.NewTCPServer(address, dispatcher.NewHandlerFunc())
	if servers == nil {
		servers = make(map[string]*gorpc.Server)
	}
	servers[address] = s
	return s.Serve()
}

// StopServer halts the given server.
func StopServer(address string) error {
	if servers == nil {
		return ErrNoServerRunning
	}
	s, found := servers[address]
	if !found {
		return ErrServerNotFound
	}
	delete(servers, address)
	s.Stop()
	return nil
}

// Shutdown halts all RPC servers.
func Shutdown() {
	for _, s := range servers {
		s.Stop()
	}
	dvid.Infof("Halted %d RPC servers.\n", len(servers))
	servers = nil
}
