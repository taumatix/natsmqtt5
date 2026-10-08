package natsmqtt5

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/packet"
)

func TestTheInflightStateRoundTripsThroughARecord(t *testing.T) {
	rec := &sessionRecord{Version: sessionRecordVersion, ClientID: "c",
		Inflight: []storedInflight{
			{ID: 7, QoS: packet.QoS1, Seq: 12, Retain: true, SubID: 3},
			{ID: 9, QoS: packet.QoS2, Rel: true},
		},
		ReceivedQoS2: []uint16{4, 5},
	}
	got, err := decodeRecord(encodeRecord(rec))
	require.NoError(t, err)
	assert.Equal(t, rec.Inflight, got.Inflight)
	assert.Equal(t, rec.ReceivedQoS2, got.ReceivedQoS2)

	empty := string(encodeRecord(&sessionRecord{Version: sessionRecordVersion}))
	assert.NotContains(t, empty, "Inflight")
	assert.NotContains(t, empty, "ReceivedQoS2")
}

func TestFitRecordCutsTheNewestInflightEntriesFirstAndOnlyAsFarAsNeeded(t *testing.T) {
	rec := &sessionRecord{Version: sessionRecordVersion, ClientID: "c"}
	for i := 1; i <= 1000; i++ {
		rec.Inflight = append(rec.Inflight, storedInflight{ID: uint16(i), QoS: packet.QoS1, Seq: uint64(100000 + i)})
	}
	rec.ReceivedQoS2 = []uint16{1, 2, 3}

	full := len(encodeRecord(rec))
	cutInflight, cutQoS2 := fitRecord(rec, full)
	assert.Zero(t, cutInflight+cutQoS2, "a record that fits is left alone")

	limit := full / 2
	cutInflight, cutQoS2 = fitRecord(rec, limit)
	assert.LessOrEqual(t, len(encodeRecord(rec)), limit)
	assert.Positive(t, cutInflight)
	assert.Zero(t, cutQoS2, "received identifiers are only cut once no in-flight entry is left to")
	assert.Equal(t, 1000-cutInflight, len(rec.Inflight))
	assert.Equal(t, uint16(1), rec.Inflight[0].ID, "the oldest entries are the ones kept")
	assert.Greater(t, len(rec.Inflight), 400, "not much more than needed is cut")
}

func TestFitRecordCutsReceivedIdentifiersOnceNoInflightEntryIsLeft(t *testing.T) {
	rec := &sessionRecord{Version: sessionRecordVersion, ClientID: "c",
		Inflight: []storedInflight{{ID: 1, QoS: packet.QoS1, Seq: 1}}}
	for i := 1; i <= 5000; i++ {
		rec.ReceivedQoS2 = append(rec.ReceivedQoS2, uint16(i))
	}
	cutInflight, cutQoS2 := fitRecord(rec, 2000)
	assert.Equal(t, 1, cutInflight)
	assert.Positive(t, cutQoS2)
	assert.LessOrEqual(t, len(encodeRecord(rec)), 2000)
}
