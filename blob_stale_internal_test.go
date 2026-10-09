package natsmqtt5

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestABlobWrittenInTheFutureOrNeverIsStale(t *testing.T) {
	s := &sessionStore{maxSessionExpiry: 4 * time.Hour}
	assert.True(t, s.blobStale(time.Time{}), "never written")
	assert.True(t, s.blobStale(time.Now().Add(time.Hour)), "another broker's clock is ahead")
	assert.False(t, s.blobStale(time.Now().Add(-time.Minute)), "written a minute ago")
	assert.True(t, s.blobStale(time.Now().Add(-3*time.Hour)), "older than a quarter of the TTL (2h)")
}
