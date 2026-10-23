package natsmqtt5_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
)

// startWebSocket serves the broker's WebSocket handler on a real HTTP server and
// returns its ws:// URL.
func startWebSocket(t *testing.T, b *natsmqtt5.Broker) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("/mqtt", b.WebSocketHandler())
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http") + "/mqtt"
}

// connectWS is connectClient over a WebSocket: a Paho client on the binary-framed
// connection, the way a browser or a hosted client reaches the broker.
func connectWS(t *testing.T, url string, cp *paho.Connect, customise ...func(*paho.ClientConfig)) *testClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{Subprotocols: []string{"mqtt"}})
	require.NoError(t, err)
	require.Equal(t, "mqtt", ws.Subprotocol())
	ws.SetReadLimit(-1)
	nc := websocket.NetConn(context.Background(), ws, websocket.MessageBinary)

	tc := &testClient{
		t:           t,
		nc:          nc,
		messages:    make(chan received, 64),
		disconnects: make(chan *paho.Disconnect, 4),
	}
	cfg := paho.ClientConfig{
		Conn:               nc,
		ClientID:           cp.ClientID,
		OnServerDisconnect: func(d *paho.Disconnect) { tc.disconnects <- d },
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){
			func(pr paho.PublishReceived) (bool, error) {
				tc.messages <- received{Topic: pr.Packet.Topic, Payload: string(pr.Packet.Payload), QoS: pr.Packet.QoS}
				return true, nil
			},
		},
	}
	for _, fn := range customise {
		fn(&cfg)
	}
	tc.Client = paho.NewClient(cfg)
	connack, err := tc.Client.Connect(ctx, cp)
	require.NoError(t, err)
	require.Less(t, int(connack.ReasonCode), 0x80)
	t.Cleanup(func() { _ = tc.Client.Disconnect(&paho.Disconnect{ReasonCode: 0}) })
	return tc
}

// [MQTT-6.0.0-1] (the MQTT packets cross as binary frames, both ways) and [MQTT-6.0.0-4] (the
// handshake negotiates the "mqtt" subprotocol, which connectWS asserts).
func TestAWebSocketClientAndATCPClientExchangeMessagesBothWays(t *testing.T) {
	b, tcpAddr, _ := startBrokerHandle(t, startNATS(t))
	url := startWebSocket(t, b)

	web := connectWS(t, url, connectOpts("ws-client"))
	plain, _ := connectClient(t, tcpAddr, connectOpts("tcp-client"))

	_, err := web.Subscribe(context.Background(), &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: "ws/in", QoS: 1}}})
	require.NoError(t, err)
	_, err = plain.Subscribe(context.Background(), &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: "ws/out", QoS: 1}}})
	require.NoError(t, err)

	_, err = plain.Publish(context.Background(), &paho.Publish{Topic: "ws/in", QoS: 1, Payload: []byte("to the browser")})
	require.NoError(t, err)
	m := web.expectMessage()
	assert.Equal(t, "ws/in", m.Topic)
	assert.Equal(t, "to the browser", m.Payload)

	_, err = web.Publish(context.Background(), &paho.Publish{Topic: "ws/out", QoS: 1, Payload: []byte("from the browser")})
	require.NoError(t, err)
	m = plain.expectMessage()
	assert.Equal(t, "ws/out", m.Topic)
	assert.Equal(t, "from the browser", m.Payload)
}

func TestAWebSocketClientCanSendAndReceiveAPayloadBiggerThanOneFrameBuffer(t *testing.T) {
	b, tcpAddr, _ := startBrokerHandle(t, startNATS(t))
	url := startWebSocket(t, b)
	web := connectWS(t, url, connectOpts("ws-big"))
	plain, _ := connectClient(t, tcpAddr, connectOpts("tcp-big"))
	_, err := plain.Subscribe(context.Background(), &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: "big", QoS: 1}}})
	require.NoError(t, err)

	payload := strings.Repeat("x", 512*1024)
	_, err = web.Publish(context.Background(), &paho.Publish{Topic: "big", QoS: 1, Payload: []byte(payload)})
	require.NoError(t, err)
	assert.Equal(t, len(payload), len(plain.expectMessage().Payload))
}

func TestAWebSocketSessionResumesOverTCPAndKeepsItsOfflineMessages(t *testing.T) {
	b, tcpAddr, _ := startBrokerHandle(t, startNATS(t))
	url := startWebSocket(t, b)

	cp := connectOpts("ws-resume")
	cp.CleanStart = true
	cp.Properties = &paho.ConnectProperties{SessionExpiryInterval: ptrU32(60)}
	web := connectWS(t, url, cp)
	_, err := web.Subscribe(context.Background(), &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: "resume/#", QoS: 1}}})
	require.NoError(t, err)
	_ = web.nc.Close() // the WebSocket closing handshake may report an error; the broker sees the link go

	plain, _ := connectClient(t, tcpAddr, connectOpts("tcp-pub"))
	_, err = plain.Publish(context.Background(), &paho.Publish{Topic: "resume/a", QoS: 1, Payload: []byte("while away")})
	require.NoError(t, err)

	cp2 := connectOpts("ws-resume")
	cp2.CleanStart = false
	cp2.Properties = &paho.ConnectProperties{SessionExpiryInterval: ptrU32(60)}
	back := connectWS(t, url, cp2)
	m := back.expectMessage()
	assert.Equal(t, "resume/a", m.Topic)
	assert.Equal(t, "while away", m.Payload)
}

func TestAWebSocketClientIsDisconnectedWhenTheBrokerCloses(t *testing.T) {
	b, _, stop := startBrokerHandle(t, startNATS(t))
	url := startWebSocket(t, b)
	web := connectWS(t, url, connectOpts("ws-shutdown"))

	go stop()
	select {
	case d := <-web.disconnects:
		assert.Equal(t, byte(0x8B), d.ReasonCode, "Server shutting down")
	case <-time.After(10 * time.Second):
		t.Fatal("the WebSocket client was never told the broker is shutting down")
	}
}

// [MQTT-6.0.0-4]: a client that does not offer "mqtt" is refused.
func TestAWebSocketUpgradeWithoutTheMQTTSubprotocolIsRefused(t *testing.T) {
	b, _, _ := startBrokerHandle(t, startNATS(t))
	url := startWebSocket(t, b)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ws, _, err := websocket.Dial(ctx, url, nil)
	require.NoError(t, err, "the upgrade itself succeeds; the broker closes it")
	_, _, err = ws.Read(ctx)
	require.Error(t, err)
	assert.Equal(t, websocket.StatusPolicyViolation, websocket.CloseStatus(err))
}

func TestAWebSocketUpgradeFromAnotherOriginIsRefusedUnlessAllowed(t *testing.T) {
	b, _, _ := startBrokerHandle(t, startNATS(t))
	url := startWebSocket(t, b)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	hdr := http.Header{"Origin": []string{"https://evil.example.com"}}
	_, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{Subprotocols: []string{"mqtt"}, HTTPHeader: hdr})
	require.Error(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusForbidden, resp.StatusCode)

	allowing, _, _ := startBrokerHandle(t, startNATS(t), func(o *natsmqtt5.Options) {
		o.WebSocketOriginPatterns = []string{"app.example.com"}
	})
	url2 := startWebSocket(t, allowing)
	hdr = http.Header{"Origin": []string{"https://app.example.com"}}
	ws, _, err := websocket.Dial(ctx, url2, &websocket.DialOptions{Subprotocols: []string{"mqtt"}, HTTPHeader: hdr})
	require.NoError(t, err)
	_ = ws.CloseNow()
}

func TestAWebSocketUpgradeAfterCloseIsAnswered503(t *testing.T) {
	b, _, stop := startBrokerHandle(t, startNATS(t))
	url := startWebSocket(t, b)
	stop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{Subprotocols: []string{"mqtt"}})
	require.Error(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

// [MQTT-6.0.0-1]: control packets travel in binary frames; any other data frame
// closes the connection.
func TestConformance_MQTT_6_0_0_1_ATextFrameClosesTheConnection(t *testing.T) {
	b, _, _ := startBrokerHandle(t, startNATS(t))
	url := startWebSocket(t, b)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{Subprotocols: []string{"mqtt"}})
	require.NoError(t, err)

	require.NoError(t, ws.Write(ctx, websocket.MessageText, connectPacketBytes("ws-text", true, 0)))
	_, _, err = ws.Read(ctx)
	require.Error(t, err, "no CONNACK for a CONNECT sent in a text frame")
	assert.Equal(t, websocket.StatusUnsupportedData, websocket.CloseStatus(err))
}

// [MQTT-6.0.0-2]: packets are not aligned to frames. A CONNECT split over two
// frames is one CONNECT, and a PINGREQ riding in the same frame as the
// CONNECT's tail is still answered.
func TestConformance_MQTT_6_0_0_2_PacketsNeedNotAlignWithFrames(t *testing.T) {
	b, _, _ := startBrokerHandle(t, startNATS(t))
	url := startWebSocket(t, b)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{Subprotocols: []string{"mqtt"}})
	require.NoError(t, err)
	defer ws.CloseNow()

	connect := connectPacketBytes("ws-split", true, 0)
	require.NoError(t, ws.Write(ctx, websocket.MessageBinary, connect[:5]))
	require.NoError(t, ws.Write(ctx, websocket.MessageBinary, append(append([]byte{}, connect[5:]...), 0xC0, 0x00)))

	var got []byte
	for len(got) < 6 || !bytes.HasSuffix(got, []byte{0xD0, 0x00}) { // the CONNACK, then the PINGRESP
		_, data, err := ws.Read(ctx)
		require.NoError(t, err)
		got = append(got, data...)
	}
	assert.Equal(t, byte(0x20), got[0], "CONNACK first")
	assert.Equal(t, []byte{0xD0, 0x00}, got[len(got)-2:], "then the PINGRESP")
}
