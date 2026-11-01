package natsmqtt5_test

import (
	"context"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5/v2"
	"github.com/taumatix/natsmqtt5/v2/packet"
	"github.com/taumatix/natsmqtt5/v2/scram"
)

// End to end for the scram package: a real broker, a real socket, and a SCRAM
// client written here from RFC 5802 / RFC 7677 (the package's own byte-for-byte
// check against the RFC 7677 example is in scram/scram_internal_test.go).

func withSCRAM(o *natsmqtt5.Options) {
	mk := func(pw string) scram.Credential {
		c, err := scram.NewCredential(pw, []byte("salt-"+pw), 4096)
		if err != nil {
			panic(err)
		}
		return c
	}
	creds := map[string]scram.Credential{"alice": mk("pencil"), "bob": mk("hunter2")}
	o.Auther = &scram.Auther{
		Lookup: func(_ context.Context, u string) (scram.Credential, bool, error) {
			c, ok := creds[u]
			return c, ok, nil
		},
		OwnsClientID: func(user, id string) bool { return strings.HasPrefix(id, user+"-") },
	}
}

func hm(key []byte, msg string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(msg))
	return h.Sum(nil)
}

// scramClient is the client half of SCRAM-SHA-256 without channel binding.
type scramClient struct {
	t                *testing.T
	user, password   string
	nonce, firstBare string
	serverSignature  string
}

func newSCRAMClient(t *testing.T, user, password string) *scramClient {
	return &scramClient{t: t, user: user, password: password, nonce: "clientnonce" + user}
}

func (c *scramClient) first() []byte {
	c.firstBare = "n=" + c.user + ",r=" + c.nonce
	return []byte("n,," + c.firstBare)
}

func (c *scramClient) final(serverFirst []byte) []byte {
	var r, s string
	var iters int
	for _, p := range strings.Split(string(serverFirst), ",") {
		switch {
		case strings.HasPrefix(p, "r="):
			r = p[2:]
		case strings.HasPrefix(p, "s="):
			s = p[2:]
		case strings.HasPrefix(p, "i="):
			iters, _ = strconv.Atoi(p[2:])
		}
	}
	require.True(c.t, strings.HasPrefix(r, c.nonce), "the server nonce extends the client's")
	salt, err := base64.StdEncoding.DecodeString(s)
	require.NoError(c.t, err)
	salted, err := pbkdf2.Key(sha256.New, c.password, salt, iters, sha256.Size)
	require.NoError(c.t, err)
	clientKey := hm(salted, "Client Key")
	stored := sha256.Sum256(clientKey)
	withoutProof := "c=biws,r=" + r
	authMsg := c.firstBare + "," + string(serverFirst) + "," + withoutProof
	sig := hm(stored[:], authMsg)
	proof := make([]byte, len(sig))
	for i := range sig {
		proof[i] = clientKey[i] ^ sig[i]
	}
	c.serverSignature = "v=" + base64.StdEncoding.EncodeToString(hm(hm(salted, "Server Key"), authMsg))
	return []byte(withoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof))
}

func scramConnect(id, user string, first []byte) *packet.Connect {
	cp := rawConnect(id, 0)
	cp.Properties.AuthenticationMethod = scram.Method
	cp.Properties.AuthenticationData = first
	return cp
}

func scramAuth(code packet.ReasonCode, data []byte) *packet.Auth {
	return authPkt(code, scram.Method, data)
}

func (c *rawClient) expectSCRAMAuth(code packet.ReasonCode) []byte {
	c.t.Helper()
	a, ok := c.read().(*packet.Auth)
	require.True(c.t, ok, "expected an AUTH")
	require.Equal(c.t, code, a.ReasonCode)
	require.Equal(c.t, scram.Method, a.Properties.AuthenticationMethod)
	return a.Properties.AuthenticationData
}

// Full exchange on a CONNECT [MQTT-4.12.0-2], [MQTT-4.12.0-5]: the CONNACK
// carries the server signature, which the client verifies, and the Authorizer
// sees the identity.
func TestSCRAM_ExchangeOverARealSocketAuthenticatesBothWays(t *testing.T) {
	addr := p3Broker(t, withSCRAM, func(o *natsmqtt5.Options) {
		o.Authorizer = natsmqtt5.AuthorizerFunc(func(_ context.Context, r *natsmqtt5.AuthzRequest) error {
			if !strings.HasPrefix(r.Topic, r.Identity+"/") {
				return fmt.Errorf("not %s's", r.Identity)
			}
			return nil
		})
	})
	cl := newSCRAMClient(t, "alice", "pencil")
	c := dialRaw(t, addr)
	c.send(scramConnect("alice-1", "alice", cl.first()))
	sf := c.expectSCRAMAuth(packet.ContinueAuthentication)
	c.send(scramAuth(packet.ContinueAuthentication, cl.final(sf)))
	ack := c.expectConnackCode(packet.Success)
	assert.Equal(t, scram.Method, ack.Properties.AuthenticationMethod)
	assert.Equal(t, cl.serverSignature, string(ack.Properties.AuthenticationData), "the broker proves it holds the verifier")

	c.send(&packet.Subscribe{PacketID: 1, Subscriptions: []packet.Subscription{{Filter: "alice/#", QoS: 1}, {Filter: "bob/#", QoS: 1}}})
	assert.Equal(t, []packet.ReasonCode{packet.GrantedQoS1, packet.NotAuthorized}, c.read().(*packet.Suback).ReasonCodes)
}

// A wrong password and an unknown user are refused with the same CONNACK
// [MQTT-4.12.0-4], after the same number of steps.
func TestSCRAM_WrongPasswordAndUnknownUserLookAlikeAndAreRefused(t *testing.T) {
	addr := p3Broker(t, withSCRAM)
	for _, tc := range []struct{ user, pw string }{{"alice", "wrong"}, {"mallory", "x"}} {
		cl := newSCRAMClient(t, tc.user, tc.pw)
		c := dialRaw(t, addr)
		c.send(scramConnect(tc.user+"-1", tc.user, cl.first()))
		sf := c.expectSCRAMAuth(packet.ContinueAuthentication)
		c.send(scramAuth(packet.ContinueAuthentication, cl.final(sf)))
		c.expectConnackCode(packet.BadUserNameOrPassword)
		c.expectClosed()
	}
}

// A client-first-message that is not SCRAM, one asking for channel binding, and a
// Client Identifier the user does not own end the CONNECT.
func TestSCRAM_MalformedOrUnownedClientFirstIsRefused(t *testing.T) {
	addr := p3Broker(t, withSCRAM)
	for name, tc := range map[string]struct {
		id    string
		first string
		code  packet.ReasonCode
	}{
		"garbage":         {"alice-1", "hello", packet.NotAuthorized},
		"channel binding": {"alice-1", "p=tls-unique,,n=alice,r=x", packet.NotAuthorized},
		"authzid":         {"alice-1", "n,a=root,n=alice,r=x", packet.NotAuthorized},
		"foreign id":      {"bob-1", "n,,n=alice,r=x", packet.ClientIdentifierNotValid},
	} {
		t.Run(name, func(t *testing.T) {
			c := dialRaw(t, addr)
			c.send(scramConnect(tc.id, "alice", []byte(tc.first)))
			c.expectConnackCode(tc.code)
			c.expectClosed()
		})
	}
}

// Re-authentication [MQTT-4.12.1-2] runs the exchange again on the open
// connection: AUTH 0x19 with a client-first, AUTH 0x18, AUTH 0x00 with the server
// signature. A re-authentication as another user, or with a wrong password,
// disconnects.
func TestSCRAM_Reauthentication(t *testing.T) {
	addr := p3Broker(t, withSCRAM)
	open := func(id string) *rawClient {
		cl := newSCRAMClient(t, "alice", "pencil")
		c := dialRaw(t, addr)
		c.send(scramConnect(id, "alice", cl.first()))
		c.send(scramAuth(packet.ContinueAuthentication, cl.final(c.expectSCRAMAuth(packet.ContinueAuthentication))))
		c.expectConnackCode(packet.Success)
		return c
	}
	reauth := func(c *rawClient, user, pw string) *scramClient {
		cl := newSCRAMClient(t, user, pw)
		c.send(scramAuth(packet.ReAuthenticate, cl.first()))
		return cl
	}

	c := open("alice-ok")
	cl := reauth(c, "alice", "pencil")
	c.send(scramAuth(packet.ContinueAuthentication, cl.final(c.expectSCRAMAuth(packet.ContinueAuthentication))))
	assert.Equal(t, cl.serverSignature, string(c.expectSCRAMAuth(packet.Success)))
	c.send(&packet.Pingreq{})
	_, ok := c.read().(*packet.Pingresp)
	assert.True(t, ok, "the connection carries on")

	c = open("alice-wrongpw")
	cl = reauth(c, "alice", "nope")
	c.send(scramAuth(packet.ContinueAuthentication, cl.final(c.expectSCRAMAuth(packet.ContinueAuthentication))))
	c.expectDisconnectThenClose(packet.BadUserNameOrPassword)

	c = open("alice-other")
	reauth(c, "bob", "hunter2")
	c.expectDisconnectThenClose(packet.NotAuthorized)
}

type pahoSCRAM struct{ cl *scramClient }

func (p *pahoSCRAM) Authenticate(a *paho.Auth) *paho.Auth {
	return &paho.Auth{ReasonCode: byte(packet.ContinueAuthentication),
		Properties: &paho.AuthProperties{AuthMethod: scram.Method, AuthData: p.cl.final(a.Properties.AuthData)}}
}
func (p *pahoSCRAM) Authenticated() {}

// An ordinary client library, with its own AuthHandler, completes the exchange
// and a persistent session claimed that way resumes.
func TestSCRAM_PahoClientConnectsAndTheSessionResumes(t *testing.T) {
	addr := startBroker(t, startNATS(t), persistent, withSCRAM)
	connect := func() (*testClient, *paho.Connack) {
		cl := newSCRAMClient(t, "alice", "pencil")
		cp := durableConnect("alice-durable", 300)
		cp.Properties.AuthMethod = scram.Method
		cp.Properties.AuthData = cl.first()
		return connectClient(t, addr, cp, func(c *paho.ClientConfig) { c.AuthHandler = &pahoSCRAM{cl} })
	}
	first, connack := connect()
	assert.False(t, connack.SessionPresent)
	first.subscribe(paho.SubscribeOptions{Topic: "alice/#", QoS: 1})
	require.NoError(t, first.Client.Disconnect(&paho.Disconnect{}))

	second, connack := connect()
	assert.True(t, connack.SessionPresent)
	second.publish(&paho.Publish{Topic: "alice/x", QoS: 1, Payload: []byte("resumed")})
	assert.Equal(t, "resumed", second.expectMessage().Payload)
	second.Client.Disconnect(&paho.Disconnect{})
}
