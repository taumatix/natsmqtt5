package natsmqtt5

import (
	"sync/atomic"
	"time"
)

// Sending a hole while the client stays connected (Options.ReconnectCatchUp).
//
// A live copy that never reaches the broker leaves nothing behind that says so: a route that flapped
// faster than the route watcher polls, one the server has not noticed is dead, or a connection that
// ignores discovered servers (routewatch.go) all lose copies without an event. The queue still holds
// each, and the replay cursor finds what a session was owed and never got by reading the stream
// (cursor.go): that is a hole. A hole waited for the session's next resume.
//
// So every holeCheckEvery a connection with a cursor scans for new holes, and sends those older than
// that, by the catch-up that follows a reconnect (catchUpHoles). The age is what tells a hole from a
// live copy still on its way: the scan reads the stream past messages whose live copy has not
// arrived, and one that arrives meanwhile settles its hole (noteDelivered). A hole is sent late and
// out of order, which is all that is left to do for a message its live copy missed [MQTT-4.4.0-1].
// A connection whose catch-up holds live copies back after a drop leaves the holes to its release.

// holeCheckEvery is how often holeLoop runs, and the age a hole must reach to be sent.
var holeCheckEvery atomic.Int64

func init() { holeCheckEvery.Store(int64(30 * time.Second)) }

// holeLoop runs for the life of a connection, under ReconnectCatchUp.
func (c *conn) holeLoop() {
	every := time.Duration(holeCheckEvery.Load())
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-t.C:
		}
		if c.broker.queue.Load() == nil {
			continue
		}
		if c.sess.startScan() {
			c.scanCursor()
		}
		if c.sess.hasHoleOlderThan(every) {
			c.sendHoles()
		}
	}
}

// sendHoles asks the delivery goroutine to send the session's holes.
func (c *conn) sendHoles() {
	c.catchMu.Lock()
	if c.held {
		c.catchMu.Unlock()
		return
	}
	c.withHoles, c.behind = true, true
	c.catchMu.Unlock()
	select {
	case c.catchup <- struct{}{}:
	default:
	}
}

// startScan reports whether the caller should run a cursor scan: the session has a cursor and none
// is running.
func (s *session) startScan() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cur.next == 0 || s.cur.scanning {
		return false
	}
	s.cur.scanning = true
	return true
}

// hasHoleOlderThan reports whether the cursor holds a hole recorded at least age ago.
func (s *session) hasHoleOlderThan(age time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, at := range s.cur.holeSince {
		if time.Since(at) >= age {
			return true
		}
	}
	return false
}
