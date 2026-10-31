package natsmqtt5

import (
	"log/slog"
	"strings"
	"sync"
)

// Three defaults flip in the next major version (MIGRATION.md): DurableWills,
// RestrictDollarTopics and OutboundTopicAliases. Until then each can be pinned
// with its Pin… option. An operator who pinned nothing is told once per process,
// at INFO, which options to pin to keep today's behaviour; one who set the plain
// option on, or pinned, has nothing to learn and hears nothing.

var flipNotice sync.Once

// noteComingDefaults logs the notice if any of the three is neither pinned nor
// already on.
func noteComingDefaults(l *slog.Logger, o *Options) {
	var unset []string
	if o.PinDurableWills == nil && !o.DurableWills {
		unset = append(unset, "PinDurableWills")
	}
	if o.PinRestrictDollarTopics == nil && !o.RestrictDollarTopics {
		unset = append(unset, "PinRestrictDollarTopics")
	}
	if o.PinOutboundTopicAliases == nil && !o.OutboundTopicAliases {
		unset = append(unset, "PinOutboundTopicAliases")
	}
	if len(unset) == 0 {
		return
	}
	flipNotice.Do(func() {
		l.Info("a default changes in the next major version (see MIGRATION.md); nothing changes in this one. "+
			"Set these options to keep today's behaviour or to adopt the new one now: "+strings.Join(unset, ", "),
			"options", unset)
	})
}
