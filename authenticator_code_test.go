package natsmqtt5_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/taumatix/natsmqtt5"
	"github.com/taumatix/natsmqtt5/packet"
)

// [MQTT-3.2.2-8]: "The Server sending the CONNACK packet MUST use one of the
// Connect Reason Code values" of Table 3-1. An Authenticator picks the code
// through a ConnectError, so a code Table 3-1 does not list must not reach the
// wire. It is sent as 0x80 (Unspecified error) instead; a code that does belong
// to a CONNACK passes through unchanged.

func refusingWith(code packet.ReasonCode) func(*natsmqtt5.Options) {
	return func(o *natsmqtt5.Options) {
		o.Authenticator = natsmqtt5.AuthenticatorFunc(func(context.Context, *natsmqtt5.AuthRequest) (*natsmqtt5.AuthResult, error) {
			return nil, &natsmqtt5.ConnectError{Code: code}
		})
	}
}

func TestAnAuthenticatorCodeOutsideTable31IsSentAsUnspecifiedError(t *testing.T) {
	nats := startNATS(t)
	for _, code := range []packet.ReasonCode{
		packet.Success,            // would read as an acceptance
		packet.GrantedQoS1,        // a SUBACK code
		packet.ServerShuttingDown, // 0x8B, DISCONNECT only
		packet.KeepAliveTimeout,   // 0x8D, DISCONNECT only
		packet.PacketIdentifierInUse,
		packet.SharedSubscriptionsNotSupported, // SUBACK only
		packet.ReasonCode(0xFF),
	} {
		t.Run(fmt.Sprintf("0x%02X", byte(code)), func(t *testing.T) {
			c := dialRaw(t, startBroker(t, nats, refusingWith(code)))
			c.send(connectWithMaxPacketSize("auth-code", 100))
			c.expectRawBytes([]byte{0x20, 0x03, 0x00, 0x80, 0x00})
			c.expectClosed() // [MQTT-3.2.2-7]
		})
	}
}

func TestAnAuthenticatorCodeFromTable31IsSentAsGiven(t *testing.T) {
	nats := startNATS(t)
	for _, code := range []packet.ReasonCode{
		packet.UnspecifiedError, packet.MalformedPacket, packet.ProtocolError,
		packet.ImplementationSpecificError, packet.UnsupportedProtocolVersion,
		packet.ClientIdentifierNotValid, packet.BadUserNameOrPassword,
		packet.NotAuthorized, packet.ServerUnavailable, packet.ServerBusy,
		packet.Banned, packet.BadAuthenticationMethod, packet.TopicNameInvalid,
		packet.PacketTooLarge, packet.QuotaExceeded, packet.PayloadFormatInvalid,
		packet.RetainNotSupported, packet.QoSNotSupported, packet.UseAnotherServer,
		packet.ServerMoved, packet.ConnectionRateExceeded,
	} {
		t.Run(fmt.Sprintf("0x%02X", byte(code)), func(t *testing.T) {
			c := dialRaw(t, startBroker(t, nats, refusingWith(code)))
			c.send(connectWithMaxPacketSize("auth-code", 100))
			c.expectRawBytes([]byte{0x20, 0x03, 0x00, byte(code), 0x00})
			c.expectClosed()
		})
	}
}
