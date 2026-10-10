package natsmqtt5

import (
	"context"
	"time"
)

// queueRetryEvery is how long the broker waits before trying again to create a
// default offline queue that could not be created at start-up; it doubles up to
// queueRetryMax. A var so that a test can shorten it.
var (
	queueRetryEvery = 5 * time.Second
	queueRetryMax   = time.Minute
)

// OfflineQueueActive reports whether the broker is keeping QoS 1 and 2 messages
// for disconnected sessions. It is false when the queue is off by option, and
// when the queue is on only by default and could not be created at start-up
// (the broker keeps trying, and the value turns true once it succeeds).
func (b *Broker) OfflineQueueActive() bool {
	return b.queue.Load() != nil
}

// startQueueLoops starts what depends on the queue existing: handing held
// shared messages back, and keeping shared-subscription membership alive. It is
// called once, when the queue first exists, from Open or from the retry.
func (b *Broker) startQueueLoops(q *offlineQueue) {
	b.wg.Add(1)
	// The interval is read here, on the caller's goroutine: read inside the
	// loop it races with a later test changing sharedAckWait.
	go b.holdLoop(sharedAckWait / 3)
	if q.members != nil {
		b.wg.Add(1)
		go b.refreshMembersLoop(q)
	}
}

// retryQueueLoop keeps trying to create the default offline queue that Open could
// not, so that the deviation is temporary. A session that comes and goes while
// the queue is off is not replayed anything published in that time, which is what
// the start-up warning already said.
func (b *Broker) retryQueueLoop(r *resolved) {
	defer b.wg.Done()
	wait := queueRetryEvery
	for {
		select {
		case <-b.shutdown:
			return
		case <-time.After(wait):
		}
		ctx, cancel := context.WithTimeout(context.Background(), natsFlushTimeout)
		q, err := newOfflineQueue(ctx, b.js, r)
		cancel()
		if err != nil {
			b.logger.Debug("the offline queue still cannot be created", "error", err)
			wait = min(wait*2, queueRetryMax)
			continue
		}
		b.queue.Store(q)
		b.logger.Info("the offline queue is now active: QoS 1 and 2 messages published while a session is " +
			"disconnected are kept for it from here on")
		b.startQueueLoops(q)
		return
	}
}
