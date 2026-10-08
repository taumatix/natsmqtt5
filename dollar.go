package natsmqtt5

import (
	"strings"

	"github.com/taumatix/natsmqtt5/topic"
)

// Keeping clients off "$" topics.
//
// "The Server SHOULD prevent Clients from using such Topic Names to exchange
// messages with other Clients. Server implementations MAY use Topic Names that
// start with a leading $ character for other purposes" (MQTT-5.0 §4.7.2; the
// sentence has no statement id). By default the broker follows it only for the
// two levels its own data lives under ($retained and $queue, see
// topic.ValidateName), because refusing every "$" topic would break the
// applications that use "$app/…" today. Options.RestrictDollarTopics follows
// the SHOULD for all of them.
//
// A refusal is a Topic Name invalid (0x90) on a PUBLISH, a Will or a PUBREC, and
// a Topic Filter invalid (0x8F) in a SUBACK, the codes the broker already uses
// for the two reserved levels. 0x90 is "not malformed, but is not accepted by
// this Client or Server", and 0x8F is "correctly formed but is not allowed for
// this Client"; 0x87 (Not authorized) says that this client in particular lacks
// a permission, which is not the case: no client may use the name.

// dollarName reports whether a Topic Name is one a restricted broker refuses.
func dollarName(name string) bool { return strings.HasPrefix(name, "$") }

// dollarFilter reports whether a Topic Filter is one a restricted broker
// refuses: one that starts with "$", other than a Shared Subscription
// ($share/…), and a Shared Subscription whose own Topic Filter starts with "$",
// which would otherwise be a way round the restriction. A filter that does not
// split (a malformed $share/) is for the validation that comes first to refuse.
func dollarFilter(filter string) bool {
	share, inner, err := topic.SplitShared(filter)
	if err != nil {
		return false
	}
	if share != "" {
		return strings.HasPrefix(inner, "$")
	}
	return strings.HasPrefix(filter, "$")
}
