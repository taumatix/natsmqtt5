package natsmqtt5_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/taumatix/natsmqtt5"
)

// An Authenticator is handed the TLS state of the HTTPS request that was upgraded, and the
// client's own address rather than the library's placeholder, so client-certificate
// authentication works over wss:// as it does on the TLS listener (README, "Authentication").
// MQTT-5.0 §6 names no statement for it; the test drives a real TLS socket and a real CONNECT.
func TestAnAuthenticatorSeesTheTLSStateAndRemoteAddressOfAWSSClient(t *testing.T) {
	var (
		mu   sync.Mutex
		seen *natsmqtt5.AuthRequest
	)
	auth := natsmqtt5.AuthenticatorFunc(func(_ context.Context, req *natsmqtt5.AuthRequest) (*natsmqtt5.AuthResult, error) {
		mu.Lock()
		defer mu.Unlock()
		seen = req
		return &natsmqtt5.AuthResult{Identity: "wss-user"}, nil
	})
	b, _, _ := startBrokerHandle(t, startNATS(t), func(o *natsmqtt5.Options) { o.Authenticator = auth })

	mux := http.NewServeMux()
	mux.Handle("/mqtt", b.WebSocketHandler())
	srv := httptest.NewTLSServer(mux)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	url := "wss" + strings.TrimPrefix(srv.URL, "https") + "/mqtt"
	ws, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		Subprotocols: []string{"mqtt"},
		HTTPClient:   srv.Client(),
	})
	require.NoError(t, err)
	defer ws.CloseNow()

	require.NoError(t, ws.Write(ctx, websocket.MessageBinary, connectPacketBytes("wss-client", true, 0)))
	_, connack, err := ws.Read(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(connack), 4)
	assert.Equal(t, byte(0x20), connack[0], "a CONNACK")
	assert.Equal(t, byte(0x00), connack[3], "Success")

	mu.Lock()
	defer mu.Unlock()
	require.NotNil(t, seen, "the Authenticator was called")
	require.NotNil(t, seen.TLS, "the request's TLS state reaches the Authenticator")
	assert.True(t, seen.TLS.HandshakeComplete)
	assert.GreaterOrEqual(t, seen.TLS.Version, uint16(tls.VersionTLS12))
	require.NotNil(t, seen.RemoteAddr)
	assert.True(t, strings.HasPrefix(seen.RemoteAddr.String(), "127.0.0.1:"), "the peer's address, got %q", seen.RemoteAddr)
}

// No MQTT statement bounds a WebSocket message; this is the broker's Maximum Packet Size
// applied one layer lower. A message larger than that (plus the allowance for pipelined
// packets) is refused with close status 1009 before it is buffered, not streamed to the
// packet reader.
func TestAHostileOversizeWebSocketFrameIsRejectedUpFront(t *testing.T) {
	b, _, _ := startBrokerHandle(t, startNATS(t), func(o *natsmqtt5.Options) { o.MaximumPacketSize = 1024 })
	url := startWebSocket(t, b)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{Subprotocols: []string{"mqtt"}})
	require.NoError(t, err)
	defer ws.CloseNow()

	// A valid CONNECT followed by 1 MiB of PINGREQs in the same message: every packet is small and
	// well formed, so the packet reader's own size check would pass the lot and the bytes would be
	// streamed (and answered with half a million PINGRESPs).
	hostile := append(connectPacketBytes("ws-hostile", true, 0), bytes.Repeat([]byte{0xC0, 0x00}, 1<<19)...)
	_ = ws.Write(ctx, websocket.MessageBinary, hostile)
	for err == nil {
		_, _, err = ws.Read(ctx)
	}
	assert.Equal(t, websocket.StatusMessageTooBig, websocket.CloseStatus(err))
}

// A message within the Maximum Packet Size is read as before.
func TestAWebSocketMessageWithinTheMaximumPacketSizeIsRead(t *testing.T) {
	b, _, _ := startBrokerHandle(t, startNATS(t), func(o *natsmqtt5.Options) { o.MaximumPacketSize = 4096 })
	url := startWebSocket(t, b)
	c := connectWS(t, url, connectOpts("ws-fits"))
	c.subscribe(paho.SubscribeOptions{Topic: "fits/#", QoS: 0})
	_, err := c.Publish(context.Background(), &paho.Publish{Topic: "fits/a", Payload: bytes.Repeat([]byte("x"), 3000)})
	require.NoError(t, err)
	assert.Equal(t, 3000, len(c.expectMessage().Payload))
}
