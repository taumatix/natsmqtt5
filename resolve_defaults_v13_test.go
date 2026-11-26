package natsmqtt5

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// v1.3.0 turned SharedQoS2 and QueuePlainPublishes on by default (MIGRATION.md);
// a pin is the one way back, and beats the plain bool.
func TestSharedQoS2AndQueuePlainPublishesAreOnUnlessPinned(t *testing.T) {
	off := Ptr(false)
	for name, tc := range map[string]struct {
		opts          Options
		shared, plain bool
	}{
		"nothing set": {Options{}, true, true},
		"pinned off":  {Options{PinSharedQoS2: off, PinQueuePlainPublishes: off}, false, false},
		"pin beats the bool": {Options{SharedQoS2: true, QueuePlainPublishes: true,
			PinSharedQoS2: off, PinQueuePlainPublishes: off}, false, false},
		"pinned on": {Options{PinSharedQoS2: Ptr(true), PinQueuePlainPublishes: Ptr(true)}, true, true},
	} {
		t.Run(name, func(t *testing.T) {
			r, err := tc.opts.resolve()
			assert.NoError(t, err)
			assert.Equal(t, tc.shared, r.SharedQoS2)
			assert.Equal(t, tc.plain, r.QueuePlainPublishes)
		})
	}
}
