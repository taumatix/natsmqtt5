package natsmqtt5_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// These tests drive MQTT-5.0 §4.12 (enhanced authentication) and §3.15 (AUTH)
// over a real socket, with the reference hmacAuther of example_test.go. The
// statement wordings are those of the OASIS document, read 2026-10-30.

func withAuther(o *natsmqtt5.Options) {
	o.Auther = hmacAuther{secrets: map[string]string{"alice": "s3cret", "bob": "hunter2"}}
	// alice may use alice/..., bob bob/...: the Authorizer sees the identity the
	// exchange ended with.
	o.Authorizer = natsmqtt5.AuthorizerFunc(func(_ context.Context, r *natsmqtt5.AuthzRequest) error {
		if !strings.HasPrefix(r.Topic, r.Identity+"/") {
			return errors.New("not your subtree")
		}
		return nil
	})
}

func proof(secret string, nonce []byte) []byte {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write(nonce)
	return m.Sum(nil)
}

func authConnect(id, user string) *packet.Connect {
	cp := rawConnect(id, 0)
	cp.Properties.AuthenticationMethod = hmacMethod
	cp.Properties.AuthenticationData = []byte(user)
	return cp
}

func authPkt(code packet.ReasonCode, method string, data []byte) *packet.Auth {
	return &packet.Auth{ReasonCode: code, Properties: &packet.Properties{AuthenticationMethod: method, AuthenticationData: data}}
}

func (c *rawClient) expectAuth(code packet.ReasonCode) *packet.Auth {
	c.t.Helper()
	a, ok := c.read().(*packet.Auth)
	require.True(c.t, ok, "expected an AUTH")
	assert.Equal(c.t, code, a.ReasonCode)
	assert.Equal(c.t, hmacMethod, a.Properties.AuthenticationMethod, "every AUTH names the method [MQTT-4.12.0-5]")
	assert.Empty(c.t, a.Properties.ReasonString, "no Reason String [MQTT-3.15.2-2]")
	assert.Empty(c.t, a.Properties.User, "no User Property [MQTT-3.15.2-3]")
	return a
}

func (c *rawClient) expectConnackCode(code packet.ReasonCode) *packet.Connack {
	c.t.Helper()
	a, ok := c.read().(*packet.Connack)
	require.True(c.t, ok, "expected a CONNACK")
	require.Equal(c.t, code, a.ReasonCode)
	return a
}

func (c *rawClient) expectDisconnectThenClose(code packet.ReasonCode) {
	c.t.Helper()
	d, ok := c.read().(*packet.Disconnect)
	require.True(c.t, ok, "expected a DISCONNECT")
	assert.Equal(c.t, code, d.ReasonCode)
	c.expectClosed()
}

// connectedWithAuth completes the exchange for user and returns the client.
func connectedWithAuth(t *testing.T, addr, id, user, secret string) *rawClient {
	t.Helper()
	c := dialRaw(t, addr)
	c.send(authConnect(id, user))
	nonce := c.expectAuth(packet.ContinueAuthentication).Properties.AuthenticationData
	c.send(authPkt(packet.ContinueAuthentication, hmacMethod, proof(secret, nonce)))
	ack := c.expectConnackCode(packet.Success)
	assert.Equal(t, hmacMethod, ack.Properties.AuthenticationMethod)
	return c
}

// "This packet MUST contain a Reason Code of 0x18 (Continue authentication)"
// [MQTT-4.12.0-2]; "all AUTH packets, and any successful CONNACK packet MUST
// include an Authentication Method Property with the same value as in the
// CONNECT packet" [MQTT-4.12.0-5]. The CONNACK also carries the final
// Authentication Data the method produced, and the Authorizer sees the identity.
// The AUTH packets use the allowed Reason Codes [MQTT-3.15.2-1] and, as the
// broker never sends them, no Reason String [MQTT-3.15.2-2] or User Property
// [MQTT-3.15.2-3] (asserted below).
func TestEnhancedAuth_MQTT_4_12_0_2_and_5_ExchangeEndsInAConnackNamingTheMethod(t *testing.T) {
	addr := p3Broker(t, withAuther)
	c := connectedWithAuth(t, addr, "ea-ok", "alice", "s3cret")
	_ = c

	c2 := dialRaw(t, addr)
	c2.send(authConnect("ea-ok2", "alice"))
	nonce := c2.expectAuth(packet.ContinueAuthentication).Properties.AuthenticationData
	assert.Len(t, nonce, 16)
	c2.send(authPkt(packet.ContinueAuthentication, hmacMethod, proof("s3cret", nonce)))
	ack := c2.expectConnackCode(packet.Success)
	assert.Equal(t, []byte("ok"), ack.Properties.AuthenticationData)
	assert.Empty(t, ack.Properties.ReasonString)

	// Identity "alice" reaches the Authorizer.
	c2.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "alice/#", QoS: 1}, {Filter: "bob/#", QoS: 1}}})
	suback := c2.read().(*packet.Suback)
	assert.Equal(t, []packet.ReasonCode{packet.GrantedQoS1, packet.NotAuthorized}, suback.ReasonCodes)
}

// [MQTT-4.12.0-1]: with an Auther, a method it does not support is still
// answered 0x8C and closed.
func TestEnhancedAuth_MQTT_4_12_0_1_UnsupportedMethodStillRefusedWithAnAuther(t *testing.T) {
	addr := p3Broker(t, withAuther)
	c := dialRaw(t, addr)
	cp := authConnect("ea-badmethod", "alice")
	cp.Properties.AuthenticationMethod = "SCRAM-SHA-256"
	c.send(cp)
	c.expectConnackCode(packet.BadAuthenticationMethod)
	c.expectClosed()
}

// "The Server can reject the authentication at any point in this process. It MAY
// send a CONNACK with a Reason Code of 0x80 or above ... and MUST close the
// Network Connection" [MQTT-4.12.0-4]: on the first step (unknown user) and on a
// wrong proof, with the Auther's code.
func TestEnhancedAuth_MQTT_4_12_0_4_RejectionAtAnyPointIsAConnackAndAClose(t *testing.T) {
	addr := p3Broker(t, withAuther)

	c := dialRaw(t, addr)
	c.send(authConnect("ea-nouser", "mallory"))
	ack := c.expectConnackCode(packet.BadUserNameOrPassword)
	assert.False(t, ack.SessionPresent)
	c.expectClosed()

	c = dialRaw(t, addr)
	c.send(authConnect("ea-wrong", "alice"))
	c.expectAuth(packet.ContinueAuthentication)
	c.send(authPkt(packet.ContinueAuthentication, hmacMethod, []byte("not the proof")))
	c.expectConnackCode(packet.BadUserNameOrPassword)
	c.expectClosed()
}

// "The Client responds to an AUTH packet from the Server by sending a further
// AUTH packet. This packet MUST contain a Reason Code of 0x18" [MQTT-4.12.0-3]
// binds the Client; a reply that breaks it, names another method [MQTT-4.12.0-5]
// or is no AUTH at all ends the CONNECT with a CONNACK error [MQTT-4.12.0-4].
func TestEnhancedAuth_BadRepliesMidExchangeAreRefused(t *testing.T) {
	addr := p3Broker(t, withAuther)
	cases := []struct {
		name  string
		reply packet.Packet
		code  packet.ReasonCode
	}{
		{"success code", authPkt(packet.Success, hmacMethod, []byte("x")), packet.ProtocolError},
		{"re-authenticate code", authPkt(packet.ReAuthenticate, hmacMethod, []byte("x")), packet.ProtocolError},
		{"other method", authPkt(packet.ContinueAuthentication, "SCRAM-SHA-256", []byte("x")), packet.BadAuthenticationMethod},
		{"no method", &packet.Auth{ReasonCode: packet.ContinueAuthentication}, packet.BadAuthenticationMethod},
		{"other packet", &packet.Pingreq{}, packet.ProtocolError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := dialRaw(t, addr)
			c.send(authConnect("ea-badreply", "alice"))
			c.expectAuth(packet.ContinueAuthentication)
			c.send(tc.reply)
			c.expectConnackCode(tc.code)
			c.expectClosed()
		})
	}
}

// A client that starts the exchange and goes quiet is dropped after the connect
// timeout, as one that never finishes its CONNECT is.
func TestEnhancedAuth_AClientThatStallsMidExchangeIsDropped(t *testing.T) {
	addr := p3Broker(t, withAuther, func(o *natsmqtt5.Options) { o.ConnectTimeout = 300 * time.Millisecond })
	c := dialRaw(t, addr)
	c.send(authConnect("ea-stall", "alice"))
	c.expectAuth(packet.ContinueAuthentication)
	start := time.Now()
	c.expectClosed()
	assert.Less(t, time.Since(start), 3*time.Second)
}

// "If the Client does not include an Authentication Method in the CONNECT, the
// Client MUST NOT send an AUTH packet to the Server" [MQTT-4.12.0-7]: the Server
// answers a DISCONNECT 0x82, and sends no AUTH of its own [MQTT-4.12.0-6].
// Also an AUTH before any CONNACK of a plain CONNECT is no exchange.
func TestEnhancedAuth_MQTT_4_12_0_7_AuthWithoutAMethodInTheConnectIsAProtocolError(t *testing.T) {
	addr := p3Broker(t, withAuther, func(o *natsmqtt5.Options) { o.Authorizer = nil })
	c := dialRaw(t, addr)
	ack := c.connect(rawConnect("ea-plain", 0))
	assert.Empty(t, ack.Properties.AuthenticationMethod, "no method in the CONNACK [MQTT-4.12.0-6]")
	c.send(authPkt(packet.ReAuthenticate, hmacMethod, nil))
	c.expectDisconnectThenClose(packet.ProtocolError)
}

// "If the Client supplied an Authentication Method in the CONNECT packet it can
// initiate a re-authentication at any time after receiving a CONNACK ... AUTH
// packet with a Reason Code of 0x19" (§4.12.1, [MQTT-4.12.1-1] binds the Client's
// method): a re-authentication is a full exchange with a fresh nonce, ends in AUTH 0x00
// naming the method [MQTT-4.12.0-5], and the connection keeps working throughout.
func TestEnhancedAuth_MQTT_4_12_1_Reauthentication(t *testing.T) {
	addr := p3Broker(t, withAuther)
	c := connectedWithAuth(t, addr, "ea-re", "alice", "s3cret")
	c.subscribe("alice/#", packet.QoS1)

	c.send(authPkt(packet.ReAuthenticate, hmacMethod, nil))
	nonce := c.expectAuth(packet.ContinueAuthentication).Properties.AuthenticationData
	// Other packets are still served in the middle of a re-authentication.
	c.send(&packet.Pingreq{})
	_, ok := c.read().(*packet.Pingresp)
	require.True(t, ok)
	c.send(authPkt(packet.ContinueAuthentication, hmacMethod, proof("s3cret", nonce)))
	done := c.expectAuth(packet.Success)
	assert.Equal(t, []byte("ok"), done.Properties.AuthenticationData)

	// The session is unharmed and a second re-authentication works.
	pub := connectedWithAuth(t, addr, "ea-re-pub", "alice", "s3cret")
	pub.send(&packet.Publish{Topic: "alice/x", Payload: []byte("m")})
	assert.Equal(t, "m", string(c.expectPublish().Payload))
	c.send(authPkt(packet.ReAuthenticate, hmacMethod, nil))
	c.expectAuth(packet.ContinueAuthentication)
}

// "If the re-authentication fails, the Client or Server SHOULD send DISCONNECT
// with an appropriate Reason Code ... and MUST close the Network Connection"
// [MQTT-4.12.1-2]. Each failure has its own code.
func TestEnhancedAuth_MQTT_4_12_1_ReauthenticationFailureDisconnects(t *testing.T) {
	addr := p3Broker(t, withAuther)

	t.Run("wrong proof", func(t *testing.T) {
		c := connectedWithAuth(t, addr, "ea-re-bad", "alice", "s3cret")
		c.send(authPkt(packet.ReAuthenticate, hmacMethod, nil))
		c.expectAuth(packet.ContinueAuthentication)
		c.send(authPkt(packet.ContinueAuthentication, hmacMethod, []byte("nope")))
		c.expectDisconnectThenClose(packet.BadUserNameOrPassword)
	})
	t.Run("other method", func(t *testing.T) {
		c := connectedWithAuth(t, addr, "ea-re-method", "alice", "s3cret")
		c.send(authPkt(packet.ReAuthenticate, "SCRAM-SHA-256", nil))
		c.expectDisconnectThenClose(packet.BadAuthenticationMethod)
	})
	t.Run("continue without a start", func(t *testing.T) {
		c := connectedWithAuth(t, addr, "ea-re-order", "alice", "s3cret")
		c.send(authPkt(packet.ContinueAuthentication, hmacMethod, []byte("x")))
		c.expectDisconnectThenClose(packet.ProtocolError)
	})
	t.Run("success code from the client", func(t *testing.T) {
		c := connectedWithAuth(t, addr, "ea-re-code", "alice", "s3cret")
		c.send(authPkt(packet.Success, hmacMethod, nil))
		c.expectDisconnectThenClose(packet.ProtocolError)
	})
}

// [MQTT-4.12.1-2]: a re-authentication that proves a different principal would leave the
// subscriptions authorised for the first one: the connection is dropped, with
// 0x87. Here the secret of bob is proved by a connection that began as alice.
func TestEnhancedAuth_ReauthenticationCannotChangeTheIdentity(t *testing.T) {
	addr := p3Broker(t, func(o *natsmqtt5.Options) {
		withAuther(o)
		o.Auther = swapIdentity{hmacAuther{secrets: map[string]string{"alice": "s3cret", "bob": "x"}}}
	})
	c := connectedWithAuth(t, addr, "ea-swap", "alice", "s3cret")
	c.send(authPkt(packet.ReAuthenticate, hmacMethod, nil))
	nonce := c.expectAuth(packet.ContinueAuthentication).Properties.AuthenticationData
	c.send(authPkt(packet.ContinueAuthentication, hmacMethod, proof("x", nonce)))
	c.expectDisconnectThenClose(packet.NotAuthorized)
}

// swapIdentity lets a re-authentication choose another user than the connection's.
type swapIdentity struct{ hmacAuther }

func (s swapIdentity) BeginAuth(ctx context.Context, req *natsmqtt5.AuthBegin) (natsmqtt5.AuthExchange, error) {
	if req.Reauth {
		req.Identity = "bob"
	}
	return s.hmacAuther.BeginAuth(ctx, req)
}

// [MQTT-4.12.1-2]: a client that opens a re-authentication and never finishes it is dropped.
func TestEnhancedAuth_AStalledReauthenticationIsDropped(t *testing.T) {
	addr := p3Broker(t, withAuther, func(o *natsmqtt5.Options) { o.ConnectTimeout = 300 * time.Millisecond })
	c := connectedWithAuth(t, addr, "ea-re-stall", "alice", "s3cret")
	c.send(authPkt(packet.ReAuthenticate, hmacMethod, nil))
	c.expectAuth(packet.ContinueAuthentication)
	c.expectDisconnectThenClose(packet.NotAuthorized)
}

// "A client library, not our codec": Paho v5 runs the whole exchange through its
// AuthHandler, and re-authenticates with Client.Authenticate.
type pahoHMAC struct {
	secret string
	mu     sync.Mutex
	done   int
}

func (p *pahoHMAC) Authenticate(a *paho.Auth) *paho.Auth {
	return &paho.Auth{ReasonCode: byte(packet.ContinueAuthentication), Properties: &paho.AuthProperties{
		AuthMethod: hmacMethod, AuthData: proof(p.secret, a.Properties.AuthData)}}
}
func (p *pahoHMAC) Authenticated() { p.mu.Lock(); p.done++; p.mu.Unlock() }

func TestEnhancedAuth_PahoClientConnectsAndReauthenticates(t *testing.T) {
	addr := p3Broker(t, withAuther)
	h := &pahoHMAC{secret: "s3cret"}
	cp := &paho.Connect{ClientID: "ea-paho", KeepAlive: 30, CleanStart: true,
		Properties: &paho.ConnectProperties{AuthMethod: hmacMethod, AuthData: []byte("alice")}}
	cl, connack := connectClient(t, addr, cp, func(c *paho.ClientConfig) { c.AuthHandler = h })
	assert.Equal(t, hmacMethod, connack.Properties.AuthMethod)
	cl.subscribe(paho.SubscribeOptions{Topic: "alice/#", QoS: 1})

	resp, err := cl.Client.Authenticate(context.Background(), &paho.Auth{
		ReasonCode: byte(packet.ReAuthenticate), Properties: &paho.AuthProperties{AuthMethod: hmacMethod}})
	require.NoError(t, err)
	assert.True(t, resp.Success)

	cl.publish(&paho.Publish{Topic: "alice/y", QoS: 1, Payload: []byte("still here")})
	assert.Equal(t, "still here", cl.expectMessage().Payload)
}

// A persistent session claimed through enhanced authentication is resumed by the
// same principal doing it again, with its subscription live, and the filters are
// re-authorised for the identity the exchange yields.
func TestEnhancedAuth_PersistentSessionResumesAfterEnhancedAuth(t *testing.T) {
	natsURL := startNATS(t)
	addr := startBroker(t, natsURL, persistent, withAuther)
	cp := func() *paho.Connect {
		c := durableConnect("ea-durable", 300)
		c.Properties.AuthMethod = hmacMethod
		c.Properties.AuthData = []byte("alice")
		return c
	}
	cfg := func(secret string) func(*paho.ClientConfig) {
		return func(c *paho.ClientConfig) { c.AuthHandler = &pahoHMAC{secret: secret} }
	}
	first, connack := connectClient(t, addr, cp(), cfg("s3cret"))
	assert.False(t, connack.SessionPresent)
	first.subscribe(paho.SubscribeOptions{Topic: "alice/#", QoS: 1})
	require.NoError(t, first.Client.Disconnect(&paho.Disconnect{}))

	second, connack := connectClient(t, addr, cp(), cfg("s3cret"))
	assert.True(t, connack.SessionPresent, "a session claimed after enhanced auth must resume")
	pub, _ := connectClient(t, addr, cp2("ea-pub", "alice"), cfg("s3cret"))
	pub.publish(&paho.Publish{Topic: "alice/z", QoS: 1, Payload: []byte("resumed")})
	assert.Equal(t, "resumed", second.expectMessage().Payload)
}

func cp2(id, user string) *paho.Connect {
	return &paho.Connect{ClientID: id, KeepAlive: 30, CleanStart: true,
		Properties: &paho.ConnectProperties{AuthMethod: hmacMethod, AuthData: []byte(user)}}
}
