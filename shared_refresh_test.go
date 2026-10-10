package natsmqtt5_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

func memberEntries(t *testing.T, natsURL string) int {
	t.Helper()
	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	kv, err := js.KeyValue(ctx, "MQTT5_share_members")
	require.NoError(t, err)
	keys, err := kv.Keys(ctx)
	if err != nil {
		require.ErrorIs(t, err, jetstream.ErrNoKeysFound)
		return 0
	}
	return len(keys)
}

// A refresh that read a subscription before its UNSUBSCRIBE and writes after it
// must not leave an entry for a session that is no longer a member: the entry
// would keep the group, and its backlog, alive until it lapsed (§4.8.2).
func TestARefreshDoesNotRestoreAMemberThatLeft_MQTT_5_0_4_8_2(t *testing.T) {
	natsURL := startNATS(t)
	b, addr, _ := startBrokerHandle(t, natsURL, sweeping)

	m1 := dialRaw(t, addr)
	m1.connect(rawConnect("m1", 300))
	m1.subscribe(group, packet.QoS1)
	requireConsumers(t, natsURL, 1)
	require.Equal(t, 1, memberEntries(t, natsURL))

	natsmqtt5.SetBeforeMemberRefreshWrite(t, func() { m1.unsubscribe(group) })
	natsmqtt5.RefreshMembers(b)

	require.Equal(t, 0, memberEntries(t, natsURL), "the refresh wrote back the entry of a session that left")
	requireConsumers(t, natsURL, 0)
}
