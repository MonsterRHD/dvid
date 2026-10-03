//go:build !clustered && !gcloud
// +build !clustered,!gcloud

/*
	This file contains local server code supporting push/pull with optional delimiting
	using datatype-specific filters.

    TODO: When we actually have multiple varieties of target platform (cluster, gcloud managed vm, amzn ec2),
    the repo/DAG data structure needs to be cross-platform since the receiving DVID could be an entirely
    different platform.  For example, clustered DVID frontends could use an in-memory distributed
    kv store.
*/

package datastore

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/valyala/gorpc"

	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/dvid/rpc"
	"github.com/janelia-flyem/dvid/storage"
)

// PushRepo pushes a Repo to a remote DVID server at the target address.
func PushRepo(uuid dvid.UUID, target string, config dvid.Config) error {
	m := getManager()
	if m == nil {
		return ErrManagerNotInitialized
	}

	if target == "" {
		target = rpc.DefaultAddress
		dvid.Infof("No target specified for push, defaulting to %q\n", rpc.DefaultAddress)
	}

	// Get the full local repo
	thisRepo, err := m.repoFromUUID(uuid)
	if err != nil {
		return err
	}

	// Get any filter
	filter, _, err := config.GetString("filter")
	if err != nil {
		return err
	}

	// Create a repo that is tailored by the push configuration, e.g.,
	// keeping just given data instances, etc.
	v, found := m.uuidToVersion[uuid]
	if !found {
		return ErrInvalidUUID
	}
	txRepo, transmit, err := thisRepo.customize(v, config)
	if err != nil {
		return err
	}

	// Establish session with target, which may be itself
	s, err := rpc.NewSession(target, pushMessageID)
	if err != nil {
		return fmt.Errorf("unable to connect (%s) for push: %s", target, err.Error())
	}
	// Any early exit aborts the remote session so the server never publishes
	// partial results.  endAttempted is set once Close is called because a
	// failed close leaves the remote outcome unknown and must not be aborted.
	endAttempted := false
	defer func() {
		if !endAttempted {
			if err := s.Abort(); err != nil {
				dvid.Errorf("Unable to abort push session %d after failure: %v\n", s.ID(), err)
			}
		}
	}()

	// Send the repo metadata, transmit type, and other useful data for remote server.
	dvid.Infof("Sending repo %s data to %q\n", uuid, target)
	repoSerialization, err := txRepo.GobEncode()
	if err != nil {
		return err
	}
	repoMsg := repoTxMsg{
		Session:  s.ID(),
		Transmit: transmit,
		UUID:     uuid,
		Repo:     repoSerialization,
	}
	resp, err := s.Call()(sendRepoMsg, repoMsg)
	if err != nil {
		return err
	}
	if resp == nil {
		return fmt.Errorf("push unnecessary -- versions at remote are already present")
	}

	// We should get back a version set to send, or nil = send all versions.
	versions, ok := resp.(map[dvid.VersionID]struct{})
	if !ok {
		return fmt.Errorf("received response during repo push that wasn't expected set of delta versions")
	}
	dvid.Debugf("Remote sent list of %d versions to send\n", len(versions))

	// For each data instance, send the data with optional datatype-specific filtering.
	ps := &PushSession{Filter: storage.FilterSpec(filter), Versions: versions, s: &s, t: transmit}
	for _, d := range txRepo.data {
		dvid.Infof("Sending instance %q data to %q\n", d.DataName(), target)
		if err := d.PushData(ps); err != nil {
			dvid.Errorf("Aborting send of instance %q data\n", d.DataName())
			return err
		}
	}

	// Wait for the server to acknowledge that all data committed and the repo
	// was published.  A connection failure here yields a retryable outcome.
	endAttempted = true
	return s.Close()
}

/*
func Pull(repo Repo, target string, config dvid.Config) error {
	// To Pull() we initiate a push from target.
	return nil
}
*/

// PushSession encapsulates parameters necessary for DVID-to-DVID push/pull processing.
type PushSession struct {
	Filter   storage.FilterSpec
	Versions map[dvid.VersionID]struct{}

	s   *rpc.Session
	t   rpc.Transmit
	seq uint32 // sequence of the current data instance transfer
}

// StartInstancePush initiates a data instance push.  After some number of Send
// calls, the EndInstancePush must be called.  The returned sequence number
// orders this instance's messages relative to the instance FSM on the server.
func (p *PushSession) StartInstancePush(d dvid.Data) (uint32, error) {
	p.seq++
	seq := p.seq
	dmsg := DataTxInit{
		Session:    p.s.ID(),
		Seq:        seq,
		DataName:   d.DataName(),
		TypeName:   d.TypeName(),
		InstanceID: d.InstanceID(),
		Tags:       d.Tags(),
	}
	if _, err := p.s.Call()(StartDataMsg, dmsg); err != nil {
		return 0, fmt.Errorf("couldn't send data instance %q start: %v\n", d.DataName(), err)
	}
	return seq, nil
}

// SendKV sends a key-value pair.  The key-values may be buffered before sending
// for efficiency of transmission.
func (p *PushSession) SendKV(kv *storage.KeyValue) error {
	kvmsg := KVMessage{Session: p.s.ID(), Seq: p.seq, KV: *kv, Terminate: false}
	if _, err := p.s.Call()(PutKVMsg, kvmsg); err != nil {
		return fmt.Errorf("error sending key-value to remote: %v", err)
	}
	return nil
}

// EndInstancePush terminates a data instance push.  It waits for the server to
// acknowledge that all previously sent key-value pairs committed.  A repeated
// termination is idempotent.
func (p *PushSession) EndInstancePush() error {
	endmsg := KVMessage{Session: p.s.ID(), Seq: p.seq, Terminate: true}
	if _, err := p.s.Call()(PutKVMsg, endmsg); err != nil {
		return fmt.Errorf("error sending terminate data to remote: %v", err)
	}
	return nil
}

// PushData transfers all key-value pairs pertinent to the given data instance.
// Each datatype can implement filters that can restrict the transmitted key-value pairs
// based on the given FilterSpec.  Note that because of the generality of this function,
// a particular datatype implementation could be much more efficient when implementing
// filtering.  For example, the imageblk datatype could scan its key-values using the ROI
// to generate keys (since imageblk keys will likely be a vast superset of ROI spans),
// while this generic routine will scan every key-value pair for a data instance and
// query the ROI to see if this key is ok to send.
func PushData(d dvid.Data, p *PushSession) error {
	// We should be able to get the backing store (only ordered kv for now)
	store, err := GetOrderedKeyValueDB(d)
	if err != nil {
		return fmt.Errorf("unable to get backing store for data %q: %v", d.DataName(), err)
	}

	// See if this data instance implements a Send filter.
	var filter storage.Filter
	filterer, ok := d.(storage.Filterer)
	if ok {
		var err error
		filter, err = filterer.NewFilter(p.Filter)
		if err != nil {
			return err
		}
	}

	// pick any version because flatten transmit will only have one version, and all or branch transmit will
	// be looking at all versions anyway.
	if len(p.Versions) == 0 {
		return fmt.Errorf("need at least one version to send")
	}
	var v dvid.VersionID
	for v = range p.Versions {
		break
	}
	ctx := NewVersionedCtx(d, v)

	// Send the initial data instance start message
	if _, err := p.StartInstancePush(d); err != nil {
		return err
	}

	// Send this instance's key-value pairs
	var wg sync.WaitGroup
	wg.Add(1)

	// sendErr is written only by the sender goroutine below and read after
	// wg.Wait(), so no additional synchronization is required.
	var sendErr error
	failed := func() bool { return sendErr != nil }
	noteErr := func(e error) {
		if e != nil && sendErr == nil {
			sendErr = e
		}
	}

	var kvTotal, kvSent int
	var bytesTotal, bytesSent uint64
	keysOnly := false
	if p.t == rpc.TransmitFlatten {
		// Start goroutine to receive flattened key-value pairs and transmit to remote.
		ch := make(chan *storage.TKeyValue, 1000)
		go func() {
			for {
				tkv := <-ch
				if tkv == nil {
					// Only a clean drain terminates the instance; a failed
					// instance is discarded via session abort by PushRepo.
					if !failed() {
						if err := p.EndInstancePush(); err != nil {
							noteErr(fmt.Errorf("bad data %q termination: %w", d.DataName(), err))
						}
					}
					wg.Done()
					dvid.Infof("Sent %d %q key-value pairs (%s, out of %d kv pairs, %s) [flattened]\n",
						kvSent, d.DataName(), humanize.Bytes(bytesSent), kvTotal, humanize.Bytes(bytesTotal))
					return
				}
				kvTotal++
				curBytes := uint64(len(tkv.V) + len(tkv.K))
				bytesTotal += curBytes
				if failed() {
					continue // keep draining so the range query can finish
				}
				if filter != nil {
					skip, err := filter.Check(tkv)
					if err != nil {
						dvid.Errorf("problem applying filter on data %q: %v\n", d.DataName(), err)
						continue
					}
					if skip {
						continue
					}
				}
				kvSent++
				bytesSent += curBytes
				kv := storage.KeyValue{
					K: ctx.ConstructKey(tkv.K),
					V: tkv.V,
				}
				if err := p.SendKV(&kv); err != nil {
					noteErr(fmt.Errorf("bad data %q send KV: %w", d.DataName(), err))
				}
			}
		}()

		begKey, endKey := ctx.TKeyRange()
		err = store.ProcessRange(ctx, begKey, endKey, &storage.ChunkOp{}, func(c *storage.Chunk) error {
			if c == nil {
				return fmt.Errorf("received nil chunk in flatten push for data %s", d.DataName())
			}
			ch <- c.TKeyValue
			return nil
		})
		ch <- nil
		wg.Wait()
		if err != nil {
			return fmt.Errorf("error in flatten push for data %q: %v", d.DataName(), err)
		}
		if sendErr != nil {
			return sendErr
		}
	} else {
		// Start goroutine to receive all key-value pairs and transmit to remote.
		ch := make(chan *storage.KeyValue, 1000)
		go func() {
			for {
				kv := <-ch
				if kv == nil {
					if !failed() {
						if err := p.EndInstancePush(); err != nil {
							noteErr(fmt.Errorf("bad data %q termination: %w", d.DataName(), err))
						}
					}
					wg.Done()
					dvid.Infof("Sent %d %q key-value pairs (%s, out of %d kv pairs, %s)\n",
						kvSent, d.DataName(), humanize.Bytes(bytesSent), kvTotal, humanize.Bytes(bytesTotal))
					return
				}
				if !ctx.ValidKV(kv, p.Versions) {
					continue
				}
				kvTotal++
				curBytes := uint64(len(kv.V) + len(kv.K))
				bytesTotal += curBytes
				if failed() {
					continue // keep draining so the range query can finish
				}
				if filter != nil {
					tkey, err := storage.TKeyFromKey(kv.K)
					if err != nil {
						dvid.Errorf("couldn't get %q TKey from Key %v: %v\n", d.DataName(), kv.K, err)
						continue
					}
					skip, err := filter.Check(&storage.TKeyValue{K: tkey, V: kv.V})
					if err != nil {
						dvid.Errorf("problem applying filter on data %q: %v\n", d.DataName(), err)
						continue
					}
					if skip {
						continue
					}
				}
				kvSent++
				bytesSent += curBytes
				if err := p.SendKV(kv); err != nil {
					noteErr(fmt.Errorf("bad data %q send KV: %w", d.DataName(), err))
				}
			}
		}()

		begKey, endKey := ctx.KeyRange()
		if err = store.RawRangeQuery(begKey, endKey, keysOnly, ch, nil); err != nil {
			ch <- nil // range queries signal completion with nil only on success
			wg.Wait()
			return fmt.Errorf("push voxels %q range query: %v", d.DataName(), err)
		}
		ch <- nil // safe even if the backend already enqueued its own nil
		wg.Wait()
		if sendErr != nil {
			return sendErr
		}
	}
	return nil
}

var (
	pushMessageID rpc.MessageID = "datastore.Push"
)

const (
	sendRepoMsg  = "datastore.sendRepo"
	StartDataMsg = "datastore.startData"
	PutKVMsg     = "datastore.putKV"
)

func init() {
	rpc.RegisterSessionMaker(pushMessageID, rpc.NewSessionHandlerFunc(makePushSession))

	d := rpc.Dispatcher()
	d.AddFunc(sendRepoMsg, handleSendRepo)
	d.AddFunc(StartDataMsg, handleStartData)
	d.AddFunc(PutKVMsg, handlePutKV)

	gorpc.RegisterType(&repoTxMsg{})
	gorpc.RegisterType(&DataTxInit{})
	gorpc.RegisterType(&KVMessage{})
}

type repoTxMsg struct {
	Session  rpc.SessionID
	Transmit rpc.Transmit
	UUID     dvid.UUID // either the version to send if flatten, the child of a branch, or an identifier for remote root
	Repo     []byte    // serialized repo
}

type DataTxInit struct {
	Session    rpc.SessionID
	Seq        uint32 // 1-based sequence identifying this data instance within the session
	DataName   dvid.InstanceName
	DataUUID   dvid.UUID
	RootUUID   dvid.UUID
	TypeName   dvid.TypeString
	InstanceID dvid.InstanceID
	Tags       map[string]string
}

// ---- fulfills storage.DataSpec interface ----

type DataSpec struct {
	tx *DataTxInit
}

func (d *DataSpec) DataName() dvid.InstanceName {
	return d.tx.DataName
}

func (d *DataSpec) DataUUID() dvid.UUID {
	return d.tx.DataUUID
}

func (d *DataSpec) RootUUID() dvid.UUID {
	return d.tx.RootUUID
}

func (d *DataSpec) Tags() map[string]string {
	return d.tx.Tags
}

func (d *DataSpec) TypeName() dvid.TypeString {
	return d.tx.TypeName
}

// KVMessage packages a key-value pair for transmission to a remote DVID as well as control
// of the receiving FSM.
type KVMessage struct {
	Session   rpc.SessionID
	Seq       uint32 // data instance sequence this key-value belongs to
	KV        storage.KeyValue
	Terminate bool // true if this message is the last txn for this data instance and KV is invalid.
}

// withPusher routes the message to its pusher through the rpc session
// lifecycle gate, which rejects late data and counts the call as in-flight
// until termination drains it.
func withPusher(s rpc.SessionID, fn func(*pusher) error) error {
	return rpc.WithSessionHandler(s, func(h rpc.SessionHandler) error {
		p, ok := h.(*pusher)
		if !ok {
			return fmt.Errorf("handler for session %d is not expected pusher type: %T", s, h)
		}
		return fn(p)
	})
}

func handleSendRepo(m *repoTxMsg) (map[dvid.VersionID]struct{}, error) {
	var versions map[dvid.VersionID]struct{}
	err := withPusher(m.Session, func(p *pusher) error {
		var err error
		versions, err = p.readRepo(m)
		return err
	})
	return versions, err
}

func handleStartData(m *DataTxInit) error {
	return withPusher(m.Session, func(p *pusher) error {
		return p.startData(m)
	})
}

func handlePutKV(m *KVMessage) error {
	return withPusher(m.Session, func(p *pusher) error {
		return p.putData(m)
	})
}

// --- The following is the server side of a push command ----

// TODO -- If we are actively reading instead of passively taking messages, consider
// the way lexer is written:
// http://golang.org/src/pkg/text/template/parse/lex.go?h=lexer#L86

// pushInstance tracks the transfer of one data instance, identified by its
// per-session sequence number.
type pushInstance struct {
	name  dvid.InstanceName
	stats *txStats
	store storage.KeyValueDB

	// putWg counts key-value messages accepted but not yet committed.
	// terminate waits on it so the termination ack only returns once all
	// preceding data is durable.
	putWg sync.WaitGroup

	mu          sync.Mutex
	terminating bool  // termination token seen; draining outstanding puts
	terminated  bool  // termination completed
	err         error // first error observed for this instance
}

type pusher struct {
	received  uint64 // bytes received over entire push; must be first for 64-bit atomic alignment
	sessionID rpc.SessionID
	uuid      dvid.UUID
	repo      *repoT

	instanceMap dvid.InstanceMap // map from pushed to local instance ids
	versionMap  dvid.VersionMap  // map from pushed to local version ids

	// mu guards instances and closed.  received is updated atomically.
	mu        sync.Mutex
	instances map[uint32]*pushInstance
	closed    bool

	closeOnce sync.Once
	closeErr  error

	startTime time.Time
}

func makePushSession(rpc.MessageID) (rpc.SessionHandler, error) {
	dvid.Debugf("Creating push session...\n")
	return &pusher{instances: make(map[uint32]*pushInstance)}, nil
}

// --- rpc.SessionHandler interface implementation ---

func (p *pusher) ID() rpc.SessionID {
	return p.sessionID
}

func (p *pusher) Open(sid rpc.SessionID) error {
	dvid.Debugf("Push start, session %d\n", sid)
	p.mu.Lock()
	p.sessionID = sid
	if p.instances == nil {
		p.instances = make(map[uint32]*pushInstance)
	}
	p.mu.Unlock()
	p.startTime = time.Now()
	return nil
}

// Close publishes the pushed repo.  The rpc layer guarantees that, when this
// runs, every accepted data message has already committed.  Close runs at most
// once per pusher; duplicate session termination is handled idempotently by
// the rpc layer.
func (p *pusher) Close() error {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		repo := p.repo
		p.mu.Unlock()

		// A session without repo metadata (e.g., aborted before or during
		// the initial handshake) has nothing to publish.
		if repo == nil {
			return
		}

		gb := float64(atomic.LoadUint64(&p.received)) / 1000000000
		dvid.Debugf("Closing push of uuid %s: received %.1f GBytes in %s\n", repo.uuid, gb, time.Since(p.startTime))

		// Add this repo to current DVID server
		m := getManager()
		if m == nil {
			p.closeErr = ErrManagerNotInitialized
			return
		}
		p.closeErr = m.addRepo(repo)
	})
	return p.closeErr
}

func (p *pusher) readRepo(m *repoTxMsg) (map[dvid.VersionID]struct{}, error) {
	dvid.Debugf("Reading repo for push of %s...\n", m.UUID)

	mgr := getManager()
	if mgr == nil {
		return nil, ErrManagerNotInitialized
	}
	atomic.AddUint64(&p.received, uint64(len(m.Repo)))

	// Get the repo metadata
	repo := new(repoT)
	if err := repo.GobDecode(m.Repo); err != nil {
		return nil, err
	}

	// TODO: Check to see if the transmitted repo is in current metadata.

	remoteV, err := repo.versionFromUUID(m.UUID) // do this before we remap the repo's IDs
	if err != nil {
		return nil, err
	}
	repoID, err := mgr.newRepoID()
	if err != nil {
		return nil, err
	}
	repo.id = repoID

	instanceMap, versionMap, err := repo.remapLocalIDs()
	if err != nil {
		return nil, err
	}

	// After getting remote repo, adjust data instances for local settings.
	for _, d := range repo.data {
		// see if it needs to adjust versions.
		dv, needsUpdate := d.(VersionRemapper)
		if needsUpdate {
			if err := dv.RemapVersions(versionMap); err != nil {
				return nil, err
			}
		}

		// check if we have an assigned store for this data instance.
		store, err := storage.GetAssignedStore(d)
		if err != nil {
			return nil, err
		}
		d.SetKVStore(store)
		dvid.Debugf("Assigning as default store of data instance %q @ %s: %s\n", d.DataName(), d.RootUUID(), store)
	}

	var versions map[dvid.VersionID]struct{}
	switch m.Transmit {
	case rpc.TransmitFlatten:
		versions = map[dvid.VersionID]struct{}{
			remoteV: struct{}{},
		}
		// Also have to make sure any data instances are rerooted if the root
		// no longer exists.
		for name, d := range repo.data {
			_, found := mgr.uuidToVersion[d.RootUUID()]
			if !found {
				repo.data[name].SetRootUUID(m.UUID)
			}
		}
	case rpc.TransmitAll:
		versions, err = getDeltaAll(repo, m.UUID)
		if err != nil {
			return nil, err
		}
	case rpc.TransmitBranch:
		versions, err = getDeltaBranch(repo, m.UUID)
		if err != nil {
			return nil, err
		}
	}
	if versions == nil {
		// Don't retain the provisional repo: the client aborts this session
		// and nothing must be published.
		return nil, fmt.Errorf("no push required -- remote has necessary versions")
	}

	// Publish the fully prepared handshake state only once data is expected.
	p.mu.Lock()
	p.uuid = m.UUID
	p.repo = repo
	p.instanceMap = instanceMap
	p.versionMap = versionMap
	p.mu.Unlock()

	dvid.Debugf("Finished comparing repos -- requesting %d versions from source.\n", len(versions))
	return versions, nil
}

// compares remote Repo with local one, determining a list of versions that
// need to be sent from remote to bring the local DVID up-to-date.
func getDeltaAll(remote *repoT, uuid dvid.UUID) (map[dvid.VersionID]struct{}, error) {
	m := getManager()
	if m == nil {
		return nil, ErrManagerNotInitialized
	}
	// Determine all version ids of remote DAG nodes that aren't in the local DAG.
	// Since VersionID can differ among DVID servers, we need to compare using UUIDs
	// then convert to VersionID.
	delta := make(map[dvid.VersionID]struct{})
	for _, rnode := range remote.dag.nodes {
		lv, found := m.uuidToVersion[rnode.uuid]
		if found {
			dvid.Debugf("Both remote and local have uuid %s... skipping\n", rnode.uuid)
		} else {
			dvid.Debugf("Found version %s in remote not in local: sending local version id %d\n", rnode.uuid, lv)
			delta[lv] = struct{}{}
		}
	}
	return delta, nil
}

// compares remote Repo with local one, determining a list of versions that
// need to be sent from remote to bring the local DVID up-to-date.
func getDeltaBranch(remote *repoT, branch dvid.UUID) (map[dvid.VersionID]struct{}, error) {
	// TODO branch
	return nil, fmt.Errorf("Branch transmission not currently supported in DVID")
}

// startData begins the transfer of a data instance.  A repeated start with
// the same sequence is a no-op (idempotent under retransmission).
func (p *pusher) startData(d *DataTxInit) error {
	// Get the store associated with this data instance.
	ds := &DataSpec{
		tx: d,
	}
	store, err := storage.GetAssignedStore(ds)
	if err != nil {
		return err
	}
	kvstore, ok := store.(storage.KeyValueDB)
	if !ok {
		return fmt.Errorf("backend store %q for data type %q of tx data %q is not KeyValueDB-compatable", store, d.TypeName, d.DataName)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("cannot start data %q on closed push session %d", d.DataName, p.sessionID)
	}
	if _, exists := p.instances[d.Seq]; exists {
		// Idempotent retransmission of a previously accepted start.
		dvid.Debugf("Push (session %d) ignoring duplicate start for data %q (seq %d)\n", p.sessionID, d.DataName, d.Seq)
		return nil
	}
	stats := new(txStats)
	now := time.Now()
	stats.name = string(d.DataName)
	stats.firstTime = now
	stats.lastTime = now
	p.instances[d.Seq] = &pushInstance{
		name:  d.DataName,
		stats: stats,
		store: kvstore,
	}
	dvid.Debugf("Push (session %d) starting transfer of data %q (seq %d)...\n", p.sessionID, d.DataName, d.Seq)
	return nil
}

// errDataAfterTerminate is returned when key-value data arrives after the
// instance's termination token has been processed.  It is definitive: the
// data was never stored.
var errDataAfterTerminate = errors.New("key-value data arrived after data instance termination; data was not stored")

func (p *pusher) putData(kvmsg *KVMessage) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return fmt.Errorf("cannot accept data on closed push session %d", p.sessionID)
	}
	inst, found := p.instances[kvmsg.Seq]
	if !found {
		p.mu.Unlock()
		return fmt.Errorf("received data for data instance seq %d that was not started in session %d", kvmsg.Seq, p.sessionID)
	}

	// Termination token: barrier for all key-values accepted before it.
	if kvmsg.Terminate {
		if inst.terminated {
			// Idempotent duplicate termination: return the same outcome.
			err := inst.err
			p.mu.Unlock()
			return err
		}
		inst.terminating = true
		putWg := &inst.putWg
		p.mu.Unlock()

		putWg.Wait()

		p.mu.Lock()
		inst.terminating = false
		inst.terminated = true
		err := inst.err
		stats, name := inst.stats, inst.name
		p.mu.Unlock()

		dvid.Infof("Stats for transfer of data %q:\n", name)
		stats.printStats()
		return err
	}

	// Data arriving while/after termination is rejected deterministically.
	if inst.terminating || inst.terminated {
		p.mu.Unlock()
		return errDataAfterTerminate
	}
	inst.putWg.Add(1)
	p.mu.Unlock()

	if err := p.commitKV(inst, &kvmsg.KV); err != nil {
		inst.mu.Lock()
		if inst.err == nil {
			inst.err = err
		}
		inst.mu.Unlock()
		inst.putWg.Done()
		return err
	}
	inst.putWg.Done()
	return nil
}

// commitData actually remaps and stores one key-value pair.
func (p *pusher) commitKV(inst *pushInstance, kv *storage.KeyValue) error {
	// Process the key-value pair
	oldInstance, oldVersion, _, err := storage.DataKeyToLocalIDs(kv.K)
	if err != nil {
		return err
	}

	p.mu.Lock()
	instanceMap, versionMap := p.instanceMap, p.versionMap
	p.mu.Unlock()

	// Modify the transmitted key-value to have local instance and version ids.
	newInstanceID, found := instanceMap[oldInstance]
	if !found {
		return fmt.Errorf("Received key with instance id (%d) not present in repo: %v", oldInstance, instanceMap)
	}
	newVersionID, found := versionMap[oldVersion]
	if !found {
		return fmt.Errorf("Received key with version id (%d) not present in repo: %v", oldVersion, versionMap)
	}

	// Compute the updated key-value
	// TODO: When client IDs are used, need to transmit list of pertinent clients and their IDs or just use 0 as here.
	if err := storage.UpdateDataKey(kv.K, newInstanceID, newVersionID, 0); err != nil {
		return fmt.Errorf("Unable to update data key %v: %v", kv.K, err)
	}
	inst.stats.addKV(kv.K, kv.V)
	if err := inst.store.RawPut(kv.K, kv.V); err != nil {
		return fmt.Errorf("unable to store pushed key-value: %v", err)
	}
	atomic.AddUint64(&p.received, uint64(len(kv.V)+len(kv.K)))
	return nil
}

// Make a copy of a repository, customizing it via config.
// TODO -- modify data instance properties based on filters.
func (r *repoT) customize(v dvid.VersionID, config dvid.Config) (*repoT, rpc.Transmit, error) {
	// Since we can have names separated by commas, split them
	namesStr, found, err := config.GetString("data")
	if err != nil {
		return nil, rpc.TransmitUnknown, err
	}
	var datanames dvid.InstanceNames
	if found {
		for _, name := range strings.Split(namesStr, ",") {
			datanames = append(datanames, dvid.InstanceName(strings.TrimSpace(name)))
		}
	}

	// Check the transmission behavior: all, flatten, or deltas.
	var versions map[dvid.VersionID]struct{}

	transmitStr, found, err := config.GetString("transmit")
	if err != nil {
		return nil, rpc.TransmitUnknown, err
	}
	if !found {
		transmitStr = "all"
	}
	var transmit rpc.Transmit
	switch transmitStr {
	case "flatten":
		transmit = rpc.TransmitFlatten
		versions = map[dvid.VersionID]struct{}{
			v: struct{}{},
		}
	case "all":
		transmit = rpc.TransmitAll
		versions = r.versionSet()
	case "branch":
		transmit = rpc.TransmitBranch
		versions = r.versionSet()
	default:
		return nil, rpc.TransmitUnknown, fmt.Errorf("unknown transmit %s", transmitStr)
	}

	// Make a copy filtering by allowed data instances.
	r.RLock()
	defer r.RUnlock()

	dup, err := r.duplicate(versions, datanames, nil)
	return dup, transmit, err
}
