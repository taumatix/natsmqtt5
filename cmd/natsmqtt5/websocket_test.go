package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/websocket"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWebSocketFlags(t *testing.T) {
	cfg, err := parseConfig(nil, env(nil), io.Discard)
	require.NoError(t, err)
	assert.Empty(t, cfg.wsListen, "no WebSocket listener unless asked for")
	assert.Equal(t, "/mqtt", cfg.wsPath)
	assert.Nil(t, cfg.tls)

	cfg, err = parseConfig([]string{"-ws-listen", ":8083", "-ws-path", "/ws"}, env(nil), io.Discard)
	require.NoError(t, err)
	assert.Equal(t, ":8083", cfg.wsListen)
	assert.Equal(t, "/ws", cfg.wsPath)

	cfg, err = parseConfig(nil, env(map[string]string{"NATSMQTT5_WS_LISTEN": ":9"}), io.Discard)
	require.NoError(t, err)
	assert.Equal(t, ":9", cfg.wsListen, "reachable from the environment")
}

func TestTLSFlagsGoTogetherAndMustLoad(t *testing.T) {
	_, err := parseConfig([]string{"-tls-cert", "x.pem"}, env(nil), io.Discard)
	assert.ErrorContains(t, err, "go together")
	_, err = parseConfig([]string{"-tls-cert", "/nonexistent.pem", "-tls-key", "/nonexistent.key"}, env(nil), io.Discard)
	assert.ErrorContains(t, err, "-tls-cert/-tls-key")
	_, err = parseConfig([]string{"-ws-path", "mqtt"}, env(nil), io.Discard)
	assert.ErrorContains(t, err, "-ws-path")
}

// selfSigned writes a certificate for 127.0.0.1 and its key into dir and returns a pool
// that trusts it.
func selfSigned(t *testing.T, dir string) (certPath, keyPath string, pool *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "natsmqtt5 test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	certPath, keyPath = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	require.NoError(t, os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600))
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	pool = x509.NewCertPool()
	pool.AddCert(cert)
	return certPath, keyPath, pool
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	return ln.Addr().String()
}

// connectPacket is a clean-start MQTT 5 CONNECT for clientID, byte for byte from MQTT-5.0 §3.1.
func connectPacket(clientID string) []byte {
	body := []byte{0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05, 0x02, 0x00, 0x3C, 0x00}
	body = append(body, byte(len(clientID)>>8), byte(len(clientID)))
	body = append(body, clientID...)
	return append([]byte{0x10, byte(len(body))}, body...)
}

// The command, started the way an operator starts it, serves MQTT over wss:// on -ws-listen and
// over TLS on -listen, with the certificate given by -tls-cert and -tls-key: a real NATS
// server, real sockets, a real CONNECT answered by a CONNACK on each.
func TestTheCommandServesWebSocketOverTLS(t *testing.T) {
	ns, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	require.NoError(t, err)
	go ns.Start()
	require.True(t, ns.ReadyForConnections(10*time.Second))
	t.Cleanup(ns.Shutdown)

	certPath, keyPath, pool := selfSigned(t, t.TempDir())
	tcpAddr, wsAddr := freeAddr(t), freeAddr(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runContext(ctx, []string{
			"-nats", ns.ClientURL(), "-listen", tcpAddr, "-ws-listen", wsAddr,
			"-tls-cert", certPath, "-tls-key", keyPath, "-log-level", "error",
		}, env(nil), io.Discard, io.Discard)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			assert.NoError(t, err, "a clean shutdown")
		case <-time.After(10 * time.Second):
			t.Error("the command did not stop")
		}
	})

	tlsConf := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}

	// wss://
	var ws *websocket.Conn
	require.Eventually(t, func() bool {
		dctx, dcancel := context.WithTimeout(ctx, 2*time.Second)
		defer dcancel()
		var derr error
		ws, _, derr = websocket.Dial(dctx, "wss://"+wsAddr+"/mqtt", &websocket.DialOptions{
			Subprotocols: []string{"mqtt"},
			HTTPClient:   &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConf}},
		})
		return derr == nil
	}, 15*time.Second, 100*time.Millisecond, "the wss listener answers")
	defer ws.CloseNow()
	wctx, wcancel := context.WithTimeout(ctx, 5*time.Second)
	defer wcancel()
	require.NoError(t, ws.Write(wctx, websocket.MessageBinary, connectPacket("cmd-wss")))
	_, connack, err := ws.Read(wctx)
	require.NoError(t, err)
	assert.Equal(t, []byte{0x20}, connack[:1], "a CONNACK")
	assert.Equal(t, byte(0x00), connack[3], "Success")

	// mqtts on -listen: the same flags wrap the TCP listener.
	tc, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", tcpAddr, tlsConf)
	require.NoError(t, err)
	defer tc.Close()
	_, err = tc.Write(connectPacket("cmd-tls"))
	require.NoError(t, err)
	head := make([]byte, 4)
	_, err = io.ReadFull(tc, head)
	require.NoError(t, err)
	assert.Equal(t, byte(0x20), head[0])
	assert.Equal(t, byte(0x00), head[3])

	// Plain-text WebSocket on a TLS listener is not served.
	_, resp, err := websocket.Dial(wctx, "ws://"+wsAddr+"/mqtt", &websocket.DialOptions{Subprotocols: []string{"mqtt"}})
	assert.Error(t, err)
	if resp != nil {
		assert.NotEqual(t, 101, resp.StatusCode)
	}
}
