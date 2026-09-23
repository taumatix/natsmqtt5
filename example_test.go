package natsmqtt5_test

import (
	"context"
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
