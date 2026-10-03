//go:build !clustered && !gcloud
// +build !clustered,!gcloud

package datastore

import (
	"bytes"
	"encoding/gob"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/janelia-flyem/dvid/dvid"
)

func TestResolveOpGobRoundTrip(t *testing.T) {
	op := newResolveOp(
		dvid.UUID("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		[]dvid.UUID{
			dvid.UUID("11111111111111111111111111111111"),
			dvid.UUID("22222222222222222222222222222222"),
			dvid.UUID("33333333333333333333333333333333"),
		},
		[]dvid.InstanceName{"data2", "data1"},
		"resolve note",
	)
	op.phase = phaseMerging
	op.extensions[0] = dvid.UUID("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	op.extensions[2] = dvid.UUID("cccccccccccccccccccccccccccccccc")
	op.extCommitted[0] = true
	op.instDone["data1"] = true
	op.child = dvid.UUID("dddddddddddddddddddddddddddddddd")
	op.parentChildren = [][]dvid.VersionID{{5, 6}, {}, {7}}
	op.instID = []dvid.InstanceID{11, 12}
	op.instDataUUID = []dvid.UUID{
		dvid.UUID("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
		dvid.UUID("ffffffffffffffffffffffffffffffff"),
	}
	op.errMsg = "some prior error"

	enc, err := op.GobEncode()
	if err != nil {
		t.Fatalf("can't encode resolve op: %v", err)
	}
	received := new(resolveOpT)
	if err := received.GobDecode(enc); err != nil {
		t.Fatalf("can't decode resolve op: %v", err)
	}
	if !reflect.DeepEqual(op, received) {
		checks := []struct {
			name string
			a, b interface{}
		}{
			{"id", op.id, received.id},
			{"parents", op.parents, received.parents},
			{"data", op.data, received.data},
			{"note", op.note, received.note},
			{"created", op.created, received.created},
			{"updated", op.updated, received.updated},
			{"phase", op.phase, received.phase},
			{"extensions", op.extensions, received.extensions},
			{"extCommitted", op.extCommitted, received.extCommitted},
			{"instDone", op.instDone, received.instDone},
			{"child", op.child, received.child},
			{"parentChildren", op.parentChildren, received.parentChildren},
			{"instID", op.instID, received.instID},
			{"instDataUUID", op.instDataUUID, received.instDataUUID},
			{"errMsg", op.errMsg, received.errMsg},
		}
		for _, c := range checks {
			if !reflect.DeepEqual(c.a, c.b) {
				t.Errorf("field %s differs:\n a=%#v\n b=%#v", c.name, c.a, c.b)
			}
		}
		t.Fatalf("resolve op gob round-trip mismatch")
	}
}

// oldStyleRepoGob encodes a repo with all fields except the trailing
// resolveOps map, simulating repos written before this feature.
func oldStyleRepoGob(r *repoT) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	fields := []interface{}{
		r.id,
		r.uuid,
		r.alias,
		r.description,
		r.log,
		r.properties,
		r.created,
		r.updated,
		r.data,
		r.dag,
		r.passcode,
	}
	for _, f := range fields {
		if err := enc.Encode(f); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func TestResolveOpsOldRepoGob(t *testing.T) {
	root := dvid.UUID("19b87f38f873481b9f3ac688877dff0d")
	repo := newRepo(root, dvid.VersionID(1), dvid.RepoID(13), "secret")
	repo.updated = time.Now()

	oldBytes, err := oldStyleRepoGob(repo)
	if err != nil {
		t.Fatalf("can't encode old-style repo: %v", err)
	}
	received := &repoT{
		log:        []string{},
		properties: make(map[string]interface{}),
		data:       make(map[dvid.InstanceName]DataService),
	}
	if err := received.GobDecode(oldBytes); err != nil {
		t.Fatalf("old-style repo failed to decode: %v", err)
	}
	if received.resolveOps == nil {
		t.Fatal("expected non-nil, empty resolveOps after old gob decode")
	}
	if len(received.resolveOps) != 0 {
		t.Fatalf("expected no resolve ops after old gob decode, got %d", len(received.resolveOps))
	}
	if received.passcode != "secret" {
		t.Fatalf("passcode not decoded from old-style gob: %q", received.passcode)
	}
}

func TestResolveOpKey(t *testing.T) {
	root := dvid.UUID("19b87f38f873481b9f3ac688877dff0d")
	p1 := dvid.UUID("11111111111111111111111111111111")
	p2 := dvid.UUID("22222222222222222222222222222222")
	names := []dvid.InstanceName{"b", "a"}

	key := resolveOpKey(root, []dvid.UUID{p1, p2}, names)
	if len(key) != 64 {
		t.Fatalf("unexpected key length %d: %s", len(key), key)
	}
	// Same inputs, even reordered instance names, give same key.
	if k2 := resolveOpKey(root, []dvid.UUID{p1, p2}, []dvid.InstanceName{"a", "b"}); k2 != key {
		t.Fatal("instance name ordering changed the key")
	}
	// Parent order matters.
	if k2 := resolveOpKey(root, []dvid.UUID{p2, p1}, names); k2 == key {
		t.Fatal("parent reordering should change the key")
	}
	// Instance set matters.
	if k2 := resolveOpKey(root, []dvid.UUID{p1, p2}, []dvid.InstanceName{"a"}); k2 == key {
		t.Fatal("instance set change should change the key")
	}
	// Root matters.
	if k2 := resolveOpKey(dvid.UUID("99999999999999999999999999999999"), []dvid.UUID{p1, p2}, names); k2 == key {
		t.Fatal("root change should change the key")
	}
}

func TestResolveOpsNotInJSON(t *testing.T) {
	root := dvid.UUID("19b87f38f873481b9f3ac688877dff0d")
	repo := newRepo(root, dvid.VersionID(1), dvid.RepoID(13), "")
	opID := dvid.UUID("feedfacefeedfacefeedfacefeedface")
	op := newResolveOp(opID,
		[]dvid.UUID{
			dvid.UUID("11111111111111111111111111111111"),
			dvid.UUID("22222222222222222222222222222222"),
		},
		[]dvid.InstanceName{"a"},
		"note with marker resolveopmarker",
	)
	op.child = dvid.UUID("childfacechildfacechildfacechildf")
	repo.resolveOps[resolveOpKey(root, op.parents, op.data)] = op

	full, err := repo.MarshalJSON()
	if err != nil {
		t.Fatalf("can't marshal repo json: %v", err)
	}
	if strings.Contains(string(full), "feedface") || strings.Contains(string(full), "resolveopmarker") {
		t.Fatalf("resolve op leaked into full JSON: %s", full)
	}

	publicJSON, visible, err := repo.marshalJSONForVisibility(RepoPublicOnly)
	if err != nil {
		t.Fatalf("can't marshal visibility json: %v", err)
	}
	if visible && (strings.Contains(string(publicJSON), "feedface") || strings.Contains(string(publicJSON), "resolveopmarker")) {
		t.Fatalf("resolve op leaked into public JSON: %s", publicJSON)
	}
}
