package natsmqtt5_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

func TestPublishSubscribeAtEachQoS(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	for _, qos := range []byte{0, 1, 2} {
		t.Run(packet.QoS(qos).String(), func(t *testing.T) {
			sub, _ := connectClient(t, addr, connectOpts("sub-"+string('0'+qos)))
			pub, _ := connectClient(t, addr, connectOpts("pub-"+string('0'+qos)))

			ack := sub.subscribe(paho.SubscribeOptions{Topic: "sensors/temp", QoS: qos})
			require.Equal(t, []byte{qos}, ack.Reasons, "SUBACK must grant the requested QoS")

			pub.publish(&paho.Publish{Topic: "sensors/temp", QoS: qos, Payload: []byte("21.5")})

			got := sub.expectMessage()
			assert.Equal(t, "sensors/temp", got.Topic)
			assert.Equal(t, "21.5", got.Payload)
			assert.Equal(t, qos, got.QoS, "delivered QoS is min(published, granted)")
		})
	}
}

// A subscription's granted QoS caps delivery: "The QoS of Application Messages
// sent in response to a Subscription MUST be the minimum of the QoS of the
// originally published message and the Maximum QoS granted" [MQTT-3.8.4-8].
func TestDeliveredQoSIsTheMinimumOfPublishedAndGranted(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	sub, _ := connectClient(t, addr, connectOpts("sub"))
	pub, _ := connectClient(t, addr, connectOpts("pub"))

	sub.subscribe(paho.SubscribeOptions{Topic: "a/b", QoS: 0})
	pub.publish(&paho.Publish{Topic: "a/b", QoS: 2, Payload: []byte("downgraded")})

	got := sub.expectMessage()
	assert.Equal(t, byte(0), got.QoS)
	assert.Equal(t, "downgraded", got.Payload)
}

func TestWildcardSubscriptions(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	cases := []struct {
		filter   string
		matches  []string
		excludes []string
	}{
		{
			filter:   "sport/tennis/+",
			matches:  []string{"sport/tennis/player1", "sport/tennis/player2"},
			excludes: []string{"sport/tennis/player1/ranking", "sport/football/x"},
		},
		{
			// '#' includes the parent level (MQTT-5.0 §4.7.1.2), which needs a
			// second NATS subscription because '>' does not.
			filter:   "sport/#",
			matches:  []string{"sport", "sport/tennis", "sport/tennis/player1/score"},
			excludes: []string{"sports", "other/sport"},
		},
		{
			filter:   "+/+",
			matches:  []string{"/finance", "a/b"},
			excludes: []string{"a", "a/b/c"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.filter, func(t *testing.T) {
			sub, _ := connectClient(t, addr, connectOpts("sub-"+tc.filter))
			pub, _ := connectClient(t, addr, connectOpts("pub-"+tc.filter))
			sub.subscribe(paho.SubscribeOptions{Topic: tc.filter, QoS: 1})

			for _, topicName := range tc.matches {
				pub.publish(&paho.Publish{Topic: topicName, QoS: 1, Payload: []byte(topicName)})
				got := sub.expectMessage()
				assert.Equal(t, topicName, got.Topic, "%q should match %q", tc.filter, topicName)
			}
			for _, topicName := range tc.excludes {
				pub.publish(&paho.Publish{Topic: topicName, QoS: 1, Payload: []byte(topicName)})
			}
			sub.expectNoMessage()
		})
	}
}

// "The Server MUST NOT match Topic Filters starting with a wildcard character
// (# or +) with Topic Names beginning with a $ character" [MQTT-4.7.2-1].
func TestWildcardDoesNotMatchDollarTopics(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	sub, _ := connectClient(t, addr, connectOpts("sub"))
	pub, _ := connectClient(t, addr, connectOpts("pub"))

	sub.subscribe(paho.SubscribeOptions{Topic: "#", QoS: 1})
	pub.publish(&paho.Publish{Topic: "$SYS/monitor/Clients", QoS: 1, Payload: []byte("hidden")})
	sub.expectNoMessage()

	// An explicit $ prefix in the filter does match.
	sub.subscribe(paho.SubscribeOptions{Topic: "$SYS/#", QoS: 1})
	pub.publish(&paho.Publish{Topic: "$SYS/monitor/Clients", QoS: 1, Payload: []byte("visible")})
	assert.Equal(t, "visible", sub.expectMessage().Payload)
}

// Every v5 property the server must forward unaltered has to survive the trip
// through NATS headers [MQTT-3.3.2-4, -15, -16, -17, -18, -20].
func TestPropertiesSurviveTheRoundTrip(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	sub, _ := connectClient(t, addr, connectOpts("sub"))
	pub, _ := connectClient(t, addr, connectOpts("pub"))
	sub.subscribe(paho.SubscribeOptions{Topic: "req/+", QoS: 1})

	format := byte(1)
	expiry := uint32(120)
	pub.publish(&paho.Publish{
		Topic:   "req/1",
		QoS:     1,
		Payload: []byte(`{"n":1}`),
		Properties: &paho.PublishProperties{
			PayloadFormat:   &format,
			MessageExpiry:   &expiry,
			ContentType:     "application/json",
			ResponseTopic:   "reply/1",
			CorrelationData: []byte{0x00, 0xDE, 0xAD, 0xFF, '%', '\n'},
			User: paho.UserProperties{
				{Key: "site", Value: "paris"},
				{Key: "site", Value: "lyon"},
				{Key: "unit", Value: "°C"},
			},
		},
	})

	got := sub.expectMessage()
	require.NotNil(t, got.Properties)
	assert.Equal(t, "application/json", got.Properties.ContentType)
	assert.Equal(t, "reply/1", got.Properties.ResponseTopic)
	assert.Equal(t, []byte{0x00, 0xDE, 0xAD, 0xFF, '%', '\n'}, got.Properties.CorrelationData,
		"Correlation Data is arbitrary bytes and must survive header encoding")
	require.NotNil(t, got.Properties.PayloadFormat)
	assert.Equal(t, byte(1), *got.Properties.PayloadFormat)
	require.NotNil(t, got.Properties.MessageExpiry)
	assert.Equal(t, uint32(120), *got.Properties.MessageExpiry)

	// "The Server MUST maintain the order of User Properties when forwarding"
	// [MQTT-3.3.2-18], including a repeated name.
	assert.Equal(t, paho.UserProperties{
		{Key: "site", Value: "paris"},
		{Key: "site", Value: "lyon"},
		{Key: "unit", Value: "°C"},
	}, got.Properties.User)
}

// The Subscription Identifier from SUBSCRIBE comes back on matching PUBLISH
// packets [MQTT-3.3.4-3].
func TestSubscriptionIdentifierIsEchoed(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	sub, _ := connectClient(t, addr, connectOpts("sub"))
	pub, _ := connectClient(t, addr, connectOpts("pub"))

	id := 42
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := sub.Client.Subscribe(ctx, &paho.Subscribe{
		Properties:    &paho.SubscribeProperties{SubscriptionIdentifier: &id},
		Subscriptions: []paho.SubscribeOptions{{Topic: "a/b", QoS: 1}},
	})
	require.NoError(t, err)

	pub.publish(&paho.Publish{Topic: "a/b", QoS: 1, Payload: []byte("x")})

	got := sub.expectMessage()
	require.NotNil(t, got.Properties)
	require.NotNil(t, got.Properties.SubscriptionIdentifier)
	assert.Equal(t, 42, *got.Properties.SubscriptionIdentifier)
}

func TestRetainedMessages(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	t.Run("a late subscriber receives the retained message", func(t *testing.T) {
		pub, _ := connectClient(t, addr, connectOpts("pub-retain"))
		pub.publish(&paho.Publish{Topic: "state/lamp", QoS: 1, Retain: true, Payload: []byte("on")})

		sub, _ := connectClient(t, addr, connectOpts("sub-retain"))
		sub.subscribe(paho.SubscribeOptions{Topic: "state/+", QoS: 1})

		got := sub.expectMessage()
		assert.Equal(t, "state/lamp", got.Topic)
		assert.Equal(t, "on", got.Payload)
		// "These messages are sent with the RETAIN flag set to 1"
		// (MQTT-5.0 §3.3.1.3).
		assert.True(t, got.Retain)
	})

	t.Run("an empty payload clears it", func(t *testing.T) {
		pub, _ := connectClient(t, addr, connectOpts("pub-clear"))
		pub.publish(&paho.Publish{Topic: "state/door", QoS: 1, Retain: true, Payload: []byte("open")})
		// [MQTT-3.3.1-6]: a zero-byte retained publish removes the retained
		// message and is not itself stored.
		pub.publish(&paho.Publish{Topic: "state/door", QoS: 1, Retain: true, Payload: nil})

		sub, _ := connectClient(t, addr, connectOpts("sub-clear"))
		sub.subscribe(paho.SubscribeOptions{Topic: "state/door", QoS: 1})
		sub.expectNoMessage()
	})

	t.Run("Retain Handling 2 suppresses them", func(t *testing.T) {
		pub, _ := connectClient(t, addr, connectOpts("pub-rh"))
		pub.publish(&paho.Publish{Topic: "state/fan", QoS: 1, Retain: true, Payload: []byte("off")})

		sub, _ := connectClient(t, addr, connectOpts("sub-rh"))
		sub.subscribe(paho.SubscribeOptions{Topic: "state/fan", QoS: 1, RetainHandling: 2})
		sub.expectNoMessage()
	})
}

// Retain As Published decides whether a forwarded message keeps its RETAIN
// flag [MQTT-3.3.1-12, MQTT-3.3.1-13].
func TestRetainAsPublished(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	pub, _ := connectClient(t, addr, connectOpts("pub"))

	t.Run("cleared by default", func(t *testing.T) {
		sub, _ := connectClient(t, addr, connectOpts("sub-off"))
		sub.subscribe(paho.SubscribeOptions{Topic: "rap/off", QoS: 1})
		pub.publish(&paho.Publish{Topic: "rap/off", QoS: 1, Retain: true, Payload: []byte("x")})
		assert.False(t, sub.expectMessage().Retain)
	})

	t.Run("kept when requested", func(t *testing.T) {
		sub, _ := connectClient(t, addr, connectOpts("sub-on"))
		sub.subscribe(paho.SubscribeOptions{Topic: "rap/on", QoS: 1, RetainAsPublished: true})
		pub.publish(&paho.Publish{Topic: "rap/on", QoS: 1, Retain: true, Payload: []byte("x")})
		assert.True(t, sub.expectMessage().Retain)
	})
}

// "If the value is 1, Application Messages MUST NOT be forwarded to a
// connection with a ClientID equal to the ClientID of the publishing
// connection" [MQTT-3.8.3-3].
func TestNoLocal(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	self, _ := connectClient(t, addr, connectOpts("loopback"))
	other, _ := connectClient(t, addr, connectOpts("other"))

	self.subscribe(paho.SubscribeOptions{Topic: "echo", QoS: 1, NoLocal: true})

	self.publish(&paho.Publish{Topic: "echo", QoS: 1, Payload: []byte("mine")})
	self.expectNoMessage()

	other.publish(&paho.Publish{Topic: "echo", QoS: 1, Payload: []byte("theirs")})
	assert.Equal(t, "theirs", self.expectMessage().Payload)
}

// A shared subscription is a work queue: each message goes to exactly one
// member (MQTT-5.0 §4.8.2). It maps onto a NATS queue group.
func TestSharedSubscriptionDeliversToOneMember(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	a, _ := connectClient(t, addr, connectOpts("worker-a"))
	b, _ := connectClient(t, addr, connectOpts("worker-b"))
	pub, _ := connectClient(t, addr, connectOpts("producer"))

	for _, w := range []*testClient{a, b} {
		ack := w.subscribe(paho.SubscribeOptions{Topic: "$share/workers/jobs/+", QoS: 1})
		require.Equal(t, []byte{1}, ack.Reasons)
	}

	const jobs = 20
	for i := 0; i < jobs; i++ {
		pub.publish(&paho.Publish{Topic: "jobs/queue", QoS: 1, Payload: []byte("job")})
	}

	received := 0
	deadline := time.After(10 * time.Second)
	for received < jobs {
		select {
		case <-a.messages:
			received++
		case <-b.messages:
			received++
		case <-deadline:
			t.Fatalf("only %d of %d jobs were delivered", received, jobs)
		}
	}

	// Exactly-once across the group: no extra copies arrive.
	a.expectNoMessage()
	b.expectNoMessage()
}

// A QoS 2 shared subscription is granted QoS 1 instead, which the server is
// permitted to do [MQTT-3.8.4-7]. Delivery affinity for a half-finished QoS 2
// exchange cannot be expressed with a NATS queue group.
func TestSharedSubscriptionDowngradesQoS2(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	c, _ := connectClient(t, addr, connectOpts("worker"))

	ack := c.subscribe(paho.SubscribeOptions{Topic: "$share/g/jobs/#", QoS: 2})
	assert.Equal(t, []byte{1}, ack.Reasons, "SUBACK must report the downgrade")
}

func TestUnsubscribe(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	sub, _ := connectClient(t, addr, connectOpts("sub"))
	pub, _ := connectClient(t, addr, connectOpts("pub"))
	sub.subscribe(paho.SubscribeOptions{Topic: "a/#", QoS: 1})

	pub.publish(&paho.Publish{Topic: "a/b", QoS: 1, Payload: []byte("before")})
	assert.Equal(t, "before", sub.expectMessage().Payload)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ack, err := sub.Client.Unsubscribe(ctx, &paho.Unsubscribe{Topics: []string{"a/#", "never/subscribed"}})
	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x11}, ack.Reasons,
		"Success for the live subscription, 0x11 No subscription existed for the other")

	pub.publish(&paho.Publish{Topic: "a/b", QoS: 1, Payload: []byte("after")})
	sub.expectNoMessage()
}

// The Will Message is published when the connection ends without a clean
// DISCONNECT (MQTT-5.0 §3.1.2.5).
func TestLastWillAndTestament(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	watcher, _ := connectClient(t, addr, connectOpts("watcher"))
	watcher.subscribe(paho.SubscribeOptions{Topic: "status/#", QoS: 1})

	dying, _, err := tryConnect(t, addr, &paho.Connect{
		ClientID:   "dying",
		CleanStart: true,
		KeepAlive:  30,
		WillMessage: &paho.WillMessage{
			Topic:   "status/dying",
			Payload: []byte("offline"),
			QoS:     1,
		},
	})
	require.NoError(t, err)

	// Drop the socket rather than sending DISCONNECT, which is what an I/O
	// failure looks like to the broker.
	require.NoError(t, dying.Client.Disconnect(&paho.Disconnect{ReasonCode: 0x04}),
		"reason 0x04 asks the server to publish the Will anyway")

	got := watcher.expectMessage()
	assert.Equal(t, "status/dying", got.Topic)
	assert.Equal(t, "offline", got.Payload)
}

// A clean DISCONNECT with reason 0x00 deletes the Will Message
// [MQTT-3.1.2-8].
func TestNormalDisconnectDropsTheWill(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	watcher, _ := connectClient(t, addr, connectOpts("watcher"))
	watcher.subscribe(paho.SubscribeOptions{Topic: "status/#", QoS: 1})

	leaving, _, err := tryConnect(t, addr, &paho.Connect{
		ClientID:    "leaving",
		CleanStart:  true,
		KeepAlive:   30,
		WillMessage: &paho.WillMessage{Topic: "status/leaving", Payload: []byte("offline"), QoS: 1},
	})
	require.NoError(t, err)

	require.NoError(t, leaving.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}))
	watcher.expectNoMessage()
}

// A client that reconnects with Clean Start 0 inside its Session Expiry
// Interval resumes the session: CONNACK reports Session Present, and the
// subscriptions it made before are still live, so it receives messages without
// subscribing again [MQTT-3.1.2-5, MQTT-3.2.2-3].
func TestSessionResumesSubscriptions(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	pub, _ := connectClient(t, addr, connectOpts("pub"))

	resume := func() *paho.Connect {
		return &paho.Connect{
			ClientID:   "durable",
			CleanStart: false,
			KeepAlive:  30,
			Properties: &paho.ConnectProperties{SessionExpiryInterval: natsmqtt5.Ptr(uint32(300))},
		}
	}

	first, connack := connectClient(t, addr, resume())
	assert.False(t, connack.SessionPresent, "the first connection has no stored session")
	first.subscribe(paho.SubscribeOptions{Topic: "durable/#", QoS: 1})

	pub.publish(&paho.Publish{Topic: "durable/a", QoS: 1, Payload: []byte("before")})
	assert.Equal(t, "before", first.expectMessage().Payload)

	// A DISCONNECT with reason 0 ends the connection but keeps the session,
	// because the Session Expiry Interval is non-zero.
	require.NoError(t, first.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}))

	second, connack := connectClient(t, addr, resume())
	assert.True(t, connack.SessionPresent, "the stored session must be resumed")

	// No new SUBSCRIBE: the resumed subscription must still deliver.
	pub.publish(&paho.Publish{Topic: "durable/b", QoS: 1, Payload: []byte("after")})
	got := second.expectMessage()
	assert.Equal(t, "durable/b", got.Topic)
	assert.Equal(t, "after", got.Payload)
}

// Clean Start discards any stored session, so its subscriptions are gone
// [MQTT-3.1.2-4].
func TestCleanStartDiscardsTheStoredSession(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	pub, _ := connectClient(t, addr, connectOpts("pub"))

	first, _ := connectClient(t, addr, &paho.Connect{
		ClientID: "fresh", CleanStart: false, KeepAlive: 30,
		Properties: &paho.ConnectProperties{SessionExpiryInterval: natsmqtt5.Ptr(uint32(300))},
	})
	first.subscribe(paho.SubscribeOptions{Topic: "fresh/#", QoS: 1})
	require.NoError(t, first.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}))

	second, connack := connectClient(t, addr, connectOpts("fresh"))
	assert.False(t, connack.SessionPresent)

	pub.publish(&paho.Publish{Topic: "fresh/a", QoS: 1, Payload: []byte("x")})
	second.expectNoMessage()
}

// A second CONNECT with the same Client Identifier displaces the first
// [MQTT-3.1.4-3].
func TestSessionTakeover(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	disconnected := make(chan *paho.Disconnect, 1)
	nc := dial(t, addr)
	first := paho.NewClient(paho.ClientConfig{
		Conn:               nc,
		ClientID:           "twin",
		OnServerDisconnect: func(d *paho.Disconnect) { disconnected <- d },
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := first.Connect(ctx, connectOpts("twin"))
	require.NoError(t, err)

	_, _ = connectClient(t, addr, connectOpts("twin"))

	select {
	case d := <-disconnected:
		assert.Equal(t, byte(packet.SessionTakenOver), d.ReasonCode,
			"the displaced client must be told 0x8E Session taken over")
	case <-time.After(5 * time.Second):
		t.Fatal("the first connection was not displaced")
	}
}

// The CONNACK advertises every limit the broker imposes (MQTT-5.0 §3.2.2.3).
func TestConnackAdvertisesCapabilities(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	_, connack := connectClient(t, addr, connectOpts("caps"))

	require.NotNil(t, connack.Properties)
	p := connack.Properties
	assert.Equal(t, uint16(natsmqtt5.DefaultReceiveMaximum), *p.ReceiveMaximum)
	assert.Equal(t, uint32(natsmqtt5.DefaultMaximumPacketSize), *p.MaximumPacketSize)
	assert.Equal(t, uint16(natsmqtt5.DefaultTopicAliasMaximum), *p.TopicAliasMaximum)
	assert.True(t, p.RetainAvailable)
	assert.True(t, p.SharedSubAvailable)
	assert.True(t, p.WildcardSubAvailable)
	assert.True(t, p.SubIDAvailable)
	// Maximum QoS absent means 2 (MQTT-5.0 §3.2.2.3.4).
	assert.Nil(t, p.MaximumQoS)
}

// An empty Client Identifier gets one assigned, returned in CONNACK
// [MQTT-3.1.3-6, MQTT-3.1.3-7].
func TestAssignedClientIdentifier(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	_, connack := connectClient(t, addr, &paho.Connect{ClientID: "", CleanStart: true, KeepAlive: 30})

	require.NotNil(t, connack.Properties)
	assert.NotEmpty(t, connack.Properties.AssignedClientID)
}

func TestAuthenticator(t *testing.T) {
	addr := startBroker(t, startNATS(t), func(o *natsmqtt5.Options) {
		o.Authenticator = natsmqtt5.AuthenticatorFunc(
			func(_ context.Context, req *natsmqtt5.AuthRequest) (*natsmqtt5.AuthResult, error) {
				if req.Username != "alice" || string(req.Password) != "s3cret" {
					return nil, &natsmqtt5.ConnectError{
						Code:   packet.BadUserNameOrPassword,
						Reason: "unknown user",
					}
				}
				return &natsmqtt5.AuthResult{Identity: "alice"}, nil
			})
	})

	t.Run("accepted", func(t *testing.T) {
		_, connack := connectClient(t, addr, &paho.Connect{
			ClientID: "good", CleanStart: true, KeepAlive: 30,
			Username: "alice", UsernameFlag: true,
			Password: []byte("s3cret"), PasswordFlag: true,
		})
		assert.Equal(t, byte(0), connack.ReasonCode)
	})

	t.Run("refused with the reason code the authenticator chose", func(t *testing.T) {
		_, connack, err := tryConnect(t, addr, &paho.Connect{
			ClientID: "bad", CleanStart: true, KeepAlive: 30,
			Username: "mallory", UsernameFlag: true,
			Password: []byte("guess"), PasswordFlag: true,
		})
		require.Error(t, err, "Connect must fail when CONNACK carries an error")
		require.NotNil(t, connack)
		assert.Equal(t, byte(packet.BadUserNameOrPassword), connack.ReasonCode)
	})
}

func TestAuthorizerDeniesPublishAndSubscribe(t *testing.T) {
	addr := startBroker(t, startNATS(t), func(o *natsmqtt5.Options) {
		o.Authorizer = natsmqtt5.AuthorizerFunc(func(_ context.Context, req *natsmqtt5.AuthzRequest) error {
			if req.Topic == "secret" || req.Topic == "secret/#" {
				return errors.New("denied")
			}
			return nil
		})
	})

	c, _ := connectClient(t, addr, connectOpts("c"))

	ack, err := c.trySubscribe(paho.SubscribeOptions{Topic: "secret/#", QoS: 1})
	require.Error(t, err, "paho reports a refused SUBACK as an error")
	require.NotNil(t, ack)
	assert.Equal(t, []byte{byte(packet.NotAuthorized)}, ack.Reasons)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := c.Client.Publish(ctx, &paho.Publish{Topic: "secret", QoS: 1, Payload: []byte("x")})
	require.Error(t, err, "paho reports a refused PUBACK as an error")
	require.NotNil(t, resp, "the refusal arrives as a PUBACK; the connection stays up")
	assert.Equal(t, byte(packet.NotAuthorized), resp.ReasonCode)

	// The connection survives a refused publish: an allowed topic still works.
	c.publish(&paho.Publish{Topic: "public", QoS: 1, Payload: []byte("x")})
}

// A NATS-native publisher reaches MQTT subscribers, which is the point of
// putting the broker on top of NATS rather than beside it.
func TestNATSNativePublishReachesMQTTSubscriber(t *testing.T) {
	natsURL := startNATS(t)
	addr := startBroker(t, natsURL)

	sub, _ := connectClient(t, addr, connectOpts("mqtt-sub"))
	sub.subscribe(paho.SubscribeOptions{Topic: "sensors/+/temp", QoS: 1})

	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()

	// The MQTT topic "sensors/7/temp" maps to the NATS subject
	// "mqtt.sensors.7.temp" under the default prefix.
	require.NoError(t, nc.Publish("mqtt.sensors.7.temp", []byte("30.1")))
	require.NoError(t, nc.Flush())

	got := sub.expectMessage()
	assert.Equal(t, "sensors/7/temp", got.Topic)
	assert.Equal(t, "30.1", got.Payload)
	assert.Equal(t, byte(0), got.QoS, "a message with no MQTT headers arrives as QoS 0")
}

// An MQTT publisher reaches NATS-native subscribers on the mapped subject.
func TestMQTTPublishReachesNATSSubscriber(t *testing.T) {
	natsURL := startNATS(t)
	addr := startBroker(t, natsURL)

	nc, err := nats.Connect(natsURL)
	require.NoError(t, err)
	defer nc.Close()

	natsSub, err := nc.SubscribeSync("mqtt.sensors.>")
	require.NoError(t, err)
	require.NoError(t, nc.Flush())

	pub, _ := connectClient(t, addr, connectOpts("mqtt-pub"))
	pub.publish(&paho.Publish{Topic: "sensors/9/hum", QoS: 1, Payload: []byte("55")})

	msg, err := natsSub.NextMsg(5 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, "mqtt.sensors.9.hum", msg.Subject)
	assert.Equal(t, "55", string(msg.Data))
}

// Two brokers against one NATS server are one logical broker: a client on one
// reaches a subscriber on the other.
func TestTwoBrokersShareTraffic(t *testing.T) {
	natsURL := startNATS(t)
	addrA := startBroker(t, natsURL)
	addrB := startBroker(t, natsURL)

	sub, _ := connectClient(t, addrA, connectOpts("sub-on-a"))
	sub.subscribe(paho.SubscribeOptions{Topic: "cluster/#", QoS: 1})

	pub, _ := connectClient(t, addrB, connectOpts("pub-on-b"))
	pub.publish(&paho.Publish{Topic: "cluster/hello", QoS: 1, Payload: []byte("across")})

	got := sub.expectMessage()
	assert.Equal(t, "cluster/hello", got.Topic)
	assert.Equal(t, "across", got.Payload)
}

// Retained messages live in JetStream, so they are visible to a broker that
// was not running when they were published.
func TestRetainedMessagesAreSharedBetweenBrokers(t *testing.T) {
	natsURL := startNATS(t)

	pubAddr := startBroker(t, natsURL)
	pub, _ := connectClient(t, pubAddr, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "shared/state", QoS: 1, Retain: true, Payload: []byte("v1")})

	// A broker started afterwards must replay the retained set before serving.
	subAddr := startBroker(t, natsURL)
	sub, _ := connectClient(t, subAddr, connectOpts("sub"))
	sub.subscribe(paho.SubscribeOptions{Topic: "shared/state", QoS: 1})

	got := sub.expectMessage()
	assert.Equal(t, "v1", got.Payload)
	assert.True(t, got.Retain)
}

// Topics that survive the NATS subject mapping unchanged are the easy case;
// these are the awkward ones, with empty levels, dots and leading slashes.
func TestAwkwardTopicNamesRoundTrip(t *testing.T) {
	addr := startBroker(t, startNATS(t))

	sub, _ := connectClient(t, addr, connectOpts("sub"))
	pub, _ := connectClient(t, addr, connectOpts("pub"))
	sub.subscribe(paho.SubscribeOptions{Topic: "#", QoS: 1})

	for _, name := range []string{
		"/finance", "foo//bar", "a.b/c", "trailing/", "/", "//",
		"unicode/日本語", "deep/a/b/c/d/e", "ACCOUNTS", "Accounts",
	} {
		pub.publish(&paho.Publish{Topic: name, QoS: 1, Payload: []byte(name)})
		got := sub.expectMessage()
		assert.Equal(t, name, got.Topic, "topic must arrive exactly as published")
		assert.Equal(t, name, got.Payload)
	}
}

// MQTT permits a space in a Topic Name (MQTT-5.0 §4.7.3) but a NATS subject
// cannot carry one, so the broker refuses rather than mangling the topic. The
// same goes for the NATS wildcards '*' and '>', which would silently widen a
// subscription. This is the broker's documented limitation, and it fails
// loudly.
func TestTopicsNATSCannotCarryAreRefused(t *testing.T) {
	addr := startBroker(t, startNATS(t))
	pub, _ := connectClient(t, addr, connectOpts("pub"))

	for _, name := range []string{"Accounts payable", "a/*/b", "a/>/b", "tab\there"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			resp, err := pub.Client.Publish(ctx, &paho.Publish{Topic: name, QoS: 1, Payload: []byte("x")})
			require.Error(t, err)
			require.NotNil(t, resp)
			assert.Equal(t, byte(packet.TopicNameInvalid), resp.ReasonCode,
				"the client must be told 0x90 Topic Name invalid, not silently ignored")
		})
	}
}

func TestSubjectPrefixIsolatesBrokers(t *testing.T) {
	natsURL := startNATS(t)
	addrA := startBroker(t, natsURL, func(o *natsmqtt5.Options) {
		o.SubjectPrefix = "tenant-a"
		o.StreamPrefix = "TENANT_A"
	})
	addrB := startBroker(t, natsURL, func(o *natsmqtt5.Options) {
		o.SubjectPrefix = "tenant-b"
		o.StreamPrefix = "TENANT_B"
	})

	sub, _ := connectClient(t, addrA, connectOpts("sub"))
	sub.subscribe(paho.SubscribeOptions{Topic: "#", QoS: 1})

	pub, _ := connectClient(t, addrB, connectOpts("pub"))
	pub.publish(&paho.Publish{Topic: "leak/test", QoS: 1, Payload: []byte("should not arrive")})

	sub.expectNoMessage()
}

func TestRetainedMessagesCanBeDisabled(t *testing.T) {
	addr := startBroker(t, startNATS(t), func(o *natsmqtt5.Options) { o.DisableRetained = true })

	_, connack := connectClient(t, addr, connectOpts("c"))
	require.NotNil(t, connack.Properties)
	assert.False(t, connack.Properties.RetainAvailable,
		"CONNACK must advertise Retain Available 0 [MQTT-3.2.2-13]")
}

func TestMaximumQoSIsAdvertisedAndEnforced(t *testing.T) {
	addr := startBroker(t, startNATS(t), func(o *natsmqtt5.Options) {
		o.MaximumQoS = natsmqtt5.Ptr(uint8(1))
	})

	sub, connack := connectClient(t, addr, connectOpts("sub"))
	require.NotNil(t, connack.Properties.MaximumQoS)
	assert.Equal(t, byte(1), *connack.Properties.MaximumQoS)

	// "A Server that does not support QoS 2 PUBLISH packets MUST still accept
	// SUBSCRIBE packets containing a Requested QoS of 0, 1 or 2"
	// [MQTT-3.2.2-10], granting at most what it supports.
	ack := sub.subscribe(paho.SubscribeOptions{Topic: "a/b", QoS: 2})
	assert.Equal(t, []byte{1}, ack.Reasons)
}

func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	nc, err := net.DialTimeout("tcp", addr, 5*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Close() })
	return nc
}
