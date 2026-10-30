package natsmqtt5_test

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"log"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// Run a broker against an existing NATS server.
func Example() {
	broker, err := natsmqtt5.New(natsmqtt5.Options{
		NATSURL: "nats://localhost:4222",
		Listen:  ":1883",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer broker.Close()

	if err := broker.Serve(context.Background()); err != nil {
		log.Fatal(err)
	}
}

// Reject unknown credentials with the CONNACK Reason Code of your choosing,
// and confine each user to its own topic subtree.
func Example_authentication() {
	passwords := map[string]string{"alice": "s3cret"}

	opts := natsmqtt5.Options{
		NATSURL: "nats://localhost:4222",

		Authenticator: natsmqtt5.AuthenticatorFunc(
			func(_ context.Context, req *natsmqtt5.AuthRequest) (*natsmqtt5.AuthResult, error) {
				if want, ok := passwords[req.Username]; !ok || want != string(req.Password) {
					return nil, &natsmqtt5.ConnectError{Code: packet.BadUserNameOrPassword}
				}
				// A Session is keyed by its Client Identifier alone, so an
				// authenticated user must not be free to choose any: without
				// this, alice connecting as "bob-sensor" inherits bob's
				// session and everything it is subscribed to.
				//
				// A zero-length identifier is the client asking the server to
				// assign one [MQTT-3.1.3-6], so it is answered rather than
				// refused.
				if req.ClientID == "" {
					return &natsmqtt5.AuthResult{
						Identity:       req.Username,
						AssignClientID: req.Username + "/" + nuid.Next(),
					}, nil
				}
				// Equality on the first segment, not a prefix test: a user
				// named "a" passes HasPrefix("a/b/sensor", "a/") and would
				// inherit the session of the user named "a/b".
				if owner, _, found := strings.Cut(req.ClientID, "/"); !found || owner != req.Username {
					return nil, &natsmqtt5.ConnectError{Code: packet.ClientIdentifierNotValid}
				}
				return &natsmqtt5.AuthResult{Identity: req.Username}, nil
			}),

		Authorizer: natsmqtt5.AuthorizerFunc(
			func(_ context.Context, req *natsmqtt5.AuthzRequest) error {
				// A denied publish comes back as 0x87 in the PUBACK; a denied
				// subscription as 0x87 in the SUBACK. The connection stays up.
				if !strings.HasPrefix(req.Topic, "users/"+req.Identity+"/") {
					return errors.New("outside the user's subtree")
				}
				return nil
			}),
	}

	broker, err := natsmqtt5.New(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer broker.Close()

	log.Fatal(broker.Serve(context.Background()))
}

// Share one NATS connection between the broker and the rest of an application,
// and keep MQTT traffic in its own subject namespace.
func Example_embedded() {
	// An existing connection the application already uses. The broker does not
	// close a connection it did not open.
	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	broker, err := natsmqtt5.New(natsmqtt5.Options{
		Conn:          nc,
		Listen:        "127.0.0.1:1883",
		SubjectPrefix: "iot",
		StreamPrefix:  "IOT",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer broker.Close()

	// An MQTT client publishing to "sensors/7/temp" now reaches any NATS
	// subscriber on "iot.sensors.7.temp", and the other way round.
	log.Fatal(broker.Serve(context.Background()))
}

// hmacAuther is a challenge/response method in the shape of SCRAM, small enough
// to read: the client names itself in its CONNECT's Authentication Data, the
// broker answers with a random nonce in an AUTH packet, and the client proves it
// holds the shared secret by returning HMAC-SHA256(secret, nonce). The secret
// never crosses the wire. It also serves the same exchange again for a
// re-authentication, with a fresh nonce.
type hmacAuther struct{ secrets map[string]string }

const hmacMethod = "HMAC-SHA256-CHALLENGE"

func (a hmacAuther) BeginAuth(_ context.Context, req *natsmqtt5.AuthBegin) (natsmqtt5.AuthExchange, error) {
	if req.Method != hmacMethod {
		return nil, natsmqtt5.ErrBadAuthMethod
	}
	return &hmacExchange{secrets: a.secrets, reauth: req.Reauth, user: req.Identity}, nil
}

type hmacExchange struct {
	secrets map[string]string
	reauth  bool
	user    string
	nonce   []byte
}

func (e *hmacExchange) Next(_ context.Context, data []byte) (natsmqtt5.AuthStep, error) {
	refused := &natsmqtt5.ConnectError{Code: packet.BadUserNameOrPassword}
	if e.nonce == nil {
		// First input: who is asking. A re-authentication already knows.
		if !e.reauth {
			e.user = string(data)
		}
		if _, ok := e.secrets[e.user]; !ok {
			return natsmqtt5.AuthStep{}, refused
		}
		e.nonce = make([]byte, 16)
		if _, err := rand.Read(e.nonce); err != nil {
			return natsmqtt5.AuthStep{}, err
		}
		return natsmqtt5.AuthStep{Data: e.nonce}, nil
	}
	mac := hmac.New(sha256.New, []byte(e.secrets[e.user]))
	mac.Write(e.nonce)
	if !hmac.Equal(mac.Sum(nil), data) {
		return natsmqtt5.AuthStep{}, refused
	}
	return natsmqtt5.AuthStep{Done: true, Identity: e.user, Data: []byte("ok")}, nil
}

// Offer enhanced authentication: a CONNECT that names an Authentication Method
// is run through the Auther's AUTH-packet exchange, and the identity it ends
// with is the one the Authorizer sees.
func Example_enhancedAuthentication() {
	opts := natsmqtt5.Options{
		NATSURL: "nats://localhost:4222",
		Auther:  hmacAuther{secrets: map[string]string{"alice": "s3cret"}},
		Authorizer: natsmqtt5.AuthorizerFunc(func(_ context.Context, req *natsmqtt5.AuthzRequest) error {
			if !strings.HasPrefix(req.Topic, req.Identity+"/") {
				return errors.New("not your subtree")
			}
			return nil
		}),
	}
	broker, err := natsmqtt5.New(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer broker.Close()
}
