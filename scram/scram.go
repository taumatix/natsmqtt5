// Package scram is a SCRAM-SHA-256 [natsmqtt5.Auther] (RFC 5802, RFC 7677)
// for the enhanced authentication of MQTT-5.0 §4.12.
//
// The client names the method "SCRAM-SHA-256" in its CONNECT and puts the
// client-first-message in the Authentication Data. The broker answers with the
// server-first-message in an AUTH 0x18, the client replies with the
// client-final-message in another, and the server-final-message (the server
// signature, which proves the broker knows the verifier too) is the
// Authentication Data of the CONNACK, or of the AUTH 0x00 that ends a
// re-authentication. The password never crosses the wire, and the broker stores
// only a [Credential], not the password.
//
// Limits: channel binding is not offered (a "p=" gs2 header is refused, so the
// method is SCRAM-SHA-256, not -PLUS), an authorization identity ("a=") is
// refused, and the user name and password are used as given, without the SASLprep
// normalisation of RFC 4013, so a non-ASCII password must reach the broker and the
// client in the same form.
package scram

import (
	"context"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// Method is the Authentication Method a client names.
const Method = "SCRAM-SHA-256"

// DefaultIterations is the PBKDF2 iteration count NewCredential uses. RFC 7677
// requires at least 4096.
const DefaultIterations = 4096

// Credential is what the broker keeps for a user: the salted verifier, never
// the password.
type Credential struct {
	Salt       []byte
	Iterations int
	StoredKey  []byte // H(ClientKey)
	ServerKey  []byte
}

// NewCredential derives a Credential from a password. iterations of 0 means
// DefaultIterations; salt must be random and per user.
func NewCredential(password string, salt []byte, iterations int) (Credential, error) {
	if iterations == 0 {
		iterations = DefaultIterations
	}
	salted, err := pbkdf2.Key(sha256.New, password, salt, iterations, sha256.Size)
	if err != nil {
		return Credential{}, err
	}
	stored := sha256.Sum256(mac(salted, "Client Key"))
	return Credential{Salt: salt, Iterations: iterations, StoredKey: stored[:], ServerKey: mac(salted, "Server Key")}, nil
}

// Auther is a [natsmqtt5.Auther] running SCRAM-SHA-256.
type Auther struct {
	// Lookup returns the user's Credential, or false for an unknown user. An
	// unknown user is not told apart from a wrong password: the exchange runs
	// with a made-up salt and fails at the proof.
	Lookup func(ctx context.Context, user string) (Credential, bool, error)
	// OwnsClientID, when non-nil, is asked once the user is known, and a false
	// refuses the CONNECT: the Client Identifier names a session, which nothing
	// downstream protects from another principal (see [natsmqtt5.Auther]).
	OwnsClientID func(user, clientID string) bool

	// nonce and fakeSalt are overridden by tests.
	nonce    func() string
	fakeSalt func(user string) []byte
	secret   []byte
}

// BeginAuth implements [natsmqtt5.Auther].
func (a *Auther) BeginAuth(_ context.Context, req *natsmqtt5.AuthBegin) (natsmqtt5.AuthExchange, error) {
	if req.Method != Method {
		return nil, natsmqtt5.ErrBadAuthMethod
	}
	return &exchange{a: a, req: req}, nil
}

var errRefused = &natsmqtt5.ConnectError{Code: packet.NotAuthorized}

type exchange struct {
	a   *Auther
	req *natsmqtt5.AuthBegin

	user            string
	cred            Credential
	known           bool
	clientFirstBare string
	serverFirst     string
	combinedNonce   string
	asked           bool
	gs2             string
}

func (e *exchange) Next(ctx context.Context, data []byte) (natsmqtt5.AuthStep, error) {
	if !e.asked {
		e.asked = true
		return e.first(ctx, string(data))
	}
	return e.final(string(data))
}

func (e *exchange) first(ctx context.Context, msg string) (natsmqtt5.AuthStep, error) {
	// gs2-header: "n,," or "y,," then client-first-message-bare.
	hdr, bare, ok := splitGS2(msg)
	if !ok {
		return natsmqtt5.AuthStep{}, errRefused
	}
	e.gs2 = hdr
	attrs := parseAttrs(bare)
	user, cnonce := unescapeUser(attrs["n"]), attrs["r"]
	if user == "" || cnonce == "" || strings.HasPrefix(bare, "m=") {
		return natsmqtt5.AuthStep{}, errRefused
	}
	if e.req.Reauth && user != e.req.Identity {
		return natsmqtt5.AuthStep{}, errRefused
	}
	if own := e.a.OwnsClientID; own != nil && !e.req.Reauth && !own(user, e.req.ClientID) {
		return natsmqtt5.AuthStep{}, &natsmqtt5.ConnectError{Code: packet.ClientIdentifierNotValid}
	}
	cred, known, err := e.a.Lookup(ctx, user)
	if err != nil {
		return natsmqtt5.AuthStep{}, &natsmqtt5.ConnectError{Code: packet.ServerUnavailable}
	}
	if !known {
		cred = Credential{Salt: e.a.fakeSaltFor(user), Iterations: DefaultIterations}
	}
	e.user, e.cred, e.known = user, cred, known
	e.clientFirstBare = bare
	e.combinedNonce = cnonce + e.a.newNonce()
	e.serverFirst = fmt.Sprintf("r=%s,s=%s,i=%d", e.combinedNonce, base64.StdEncoding.EncodeToString(cred.Salt), cred.Iterations)
	return natsmqtt5.AuthStep{Data: []byte(e.serverFirst)}, nil
}

func (e *exchange) final(msg string) (natsmqtt5.AuthStep, error) {
	i := strings.LastIndex(msg, ",p=")
	if i < 0 || !e.known {
		return natsmqtt5.AuthStep{}, errBadProof
	}
	withoutProof := msg[:i]
	attrs := parseAttrs(withoutProof)
	proof, err := base64.StdEncoding.DecodeString(msg[i+3:])
	// c= is the base64 of the gs2 header the client sent: no channel binding.
	if err != nil || attrs["r"] != e.combinedNonce || attrs["c"] != base64.StdEncoding.EncodeToString([]byte(e.gs2)) {
		return natsmqtt5.AuthStep{}, errBadProof
	}
	authMessage := e.clientFirstBare + "," + e.serverFirst + "," + withoutProof
	clientSig := mac(e.cred.StoredKey, authMessage)
	if len(proof) != len(clientSig) {
		return natsmqtt5.AuthStep{}, errBadProof
	}
	clientKey := make([]byte, len(proof))
	for j := range proof {
		clientKey[j] = proof[j] ^ clientSig[j]
	}
	stored := sha256.Sum256(clientKey)
	if subtle.ConstantTimeCompare(stored[:], e.cred.StoredKey) != 1 {
		return natsmqtt5.AuthStep{}, errBadProof
	}
	v := base64.StdEncoding.EncodeToString(mac(e.cred.ServerKey, authMessage))
	return natsmqtt5.AuthStep{Done: true, Identity: e.user, Data: []byte("v=" + v)}, nil
}

var errBadProof = &natsmqtt5.ConnectError{Code: packet.BadUserNameOrPassword}

func (a *Auther) newNonce() string {
	if a.nonce != nil {
		return a.nonce()
	}
	b := make([]byte, 18)
	_, _ = rand.Read(b)
	return base64.RawStdEncoding.EncodeToString(b)
}

// fakeSaltFor is stable per user within a process, so an unknown user cannot be
// told from a known one by asking twice.
func (a *Auther) fakeSaltFor(user string) []byte {
	if a.fakeSalt != nil {
		return a.fakeSalt(user)
	}
	if a.secret == nil {
		a.secret = make([]byte, 32)
		_, _ = rand.Read(a.secret)
	}
	return mac(a.secret, user)[:16]
}

func mac(key []byte, msg string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(msg))
	return h.Sum(nil)
}

func splitGS2(msg string) (hdr, bare string, ok bool) {
	// "n,,", "y,,": no channel binding, no authzid.
	if !strings.HasPrefix(msg, "n,,") && !strings.HasPrefix(msg, "y,,") {
		return "", "", false
	}
	return msg[:3], msg[3:], true
}

func parseAttrs(s string) map[string]string {
	m := map[string]string{}
	for _, part := range strings.Split(s, ",") {
		if len(part) >= 2 && part[1] == '=' {
			if _, dup := m[part[:1]]; !dup {
				m[part[:1]] = part[2:]
			}
		}
	}
	return m
}

func unescapeUser(s string) string {
	return strings.NewReplacer("=2C", ",", "=3D", "=").Replace(s)
}
