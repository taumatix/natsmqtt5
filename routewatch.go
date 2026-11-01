package natsmqtt5

import (
	"slices"
	"strings"
	"time"
)

// Catching up after the NATS server's routes flap (Options.ReconnectCatchUp).
//
// A live copy is core NATS: it crosses from the publisher's server to the subscriber's over a
// route, and a route that closes drops what was in flight and what is published until it is
// back. Neither broker's own connection moves, so nats.go reports nothing. What the server does
// is send each of its clients an INFO with the cluster's client URLs when a route comes or goes,
// and nats.go keeps them as the connection's discovered servers (nats.Conn.DiscoveredServers).
// So the watcher below polls that set: a change is a route (or a server) that came or went, and
// is treated as a drop of the connection (natsLost: hold the live QoS 1 and 2 copies back), and
// once the set has stayed the same for routeSettle the sessions catch up from the queue as they
// do after a reconnect (natsBack). Each change restarts the wait, so a route that flaps twice
// is one catch-up, started after the second flap.
//
// What it does not see: a flap shorter than routePollInterval, a route the server has not
// noticed is dead (its own pings take minutes unless ping_interval is set), so what was lost
// before OfflineQueueRewind is not found, and a broker that set nats.IgnoreDiscoveredServers,
// which keeps the set from ever changing.

const (
	// routePollInterval is how often the discovered servers are read; a read takes a lock and
	// copies a few URLs.
	routePollInterval = 200 * time.Millisecond
	// routeSettle is how long the discovered servers stay the same before the catch-up starts:
	// a route that is back needs a moment to carry the interest of the other server's
	// subscriptions, and a catch-up started before then would read from a stream whose live
	// copies are still being lost.
	routeSettle = 1500 * time.Millisecond
)

// watchRoutes runs until the broker shuts down.
func (b *Broker) watchRoutes() {
	defer b.wg.Done()
	tick := time.NewTicker(routePollInterval)
	defer tick.Stop()
	known := b.routeSet()
	var changed time.Time
	for {
		select {
		case <-b.shutdown:
			return
		case <-tick.C:
		}
		if !b.nc.IsConnected() {
			// A drop of the connection is the reconnect handlers' to handle, and the set
			// it comes back with is the one to compare with.
			known, changed = b.routeSet(), time.Time{}
			continue
		}
		if now := b.routeSet(); now != known {
			known, changed = now, time.Now()
			b.logger.Info("the NATS cluster's servers changed; connected sessions will catch up from the offline queue")
			b.natsLost()
			continue
		}
		if !changed.IsZero() && time.Since(changed) >= routeSettle {
			changed = time.Time{}
			b.natsBack()
		}
	}
}

// routeSet is the servers the connection has learned of, as one comparable string.
func (b *Broker) routeSet() string {
	urls := b.nc.DiscoveredServers()
	slices.Sort(urls)
	return strings.Join(urls, " ")
}
