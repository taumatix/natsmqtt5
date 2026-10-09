package natsmqtt5

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
)

type flakyDeleteKV struct {
	jetstream.KeyValue
	failures int
	deleted  []string
}

func (k *flakyDeleteKV) Delete(_ context.Context, key string, _ ...jetstream.KVDeleteOpt) error {
	if k.failures > 0 {
		k.failures--
		return errors.New("nats: timeout")
	}
	k.deleted = append(k.deleted, key)
	return nil
}

func TestAPayloadWhoseDeleteFailedIsDeletedAtALaterCheckpoint(t *testing.T) {
	kv := &flakyDeleteKV{failures: 2}
	store := &sessionStore{blobs: kv}
	s := &session{inflight: map[uint16]*outbound{}}
	s.deadBlobs = []string{"c.aaa", "c.bbb"}

	s.reapBlobs(store)
	assert.Empty(t, kv.deleted)
	assert.ElementsMatch(t, []string{"c.aaa", "c.bbb"}, s.deadBlobs, "both are kept to try again")

	s.reapBlobs(store)
	assert.ElementsMatch(t, []string{"c.aaa", "c.bbb"}, kv.deleted)
	assert.Empty(t, s.deadBlobs)
}
