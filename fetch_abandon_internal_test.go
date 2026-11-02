package natsmqtt5

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// stuckConsumer is a consumer whose Fetch does not return, as an ordered consumer's does while it
// re-creates itself against a server that is gone.
type stuckConsumer struct {
	jetstream.Consumer
	release chan struct{}
}

func (c stuckConsumer) Fetch(int, ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	<-c.release
	return nil, context.Canceled
}

// A replay read ends with its context even when the fetch inside it cannot: Broker.Close waits for
// the read, and the library's retry inside Fetch is not cancellable.
func TestAFetchThatDoesNotReturnEndsWithItsContext(t *testing.T) {
	cons := stuckConsumer{release: make(chan struct{})}
	t.Cleanup(func() { close(cons.release) })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := (&offlineQueue{}).fetch(ctx, cons)
		done <- err
	}()
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fetch did not return after its context was cancelled")
	}
}
