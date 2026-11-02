package natsmqtt5

import (
	"testing"
	"time"
)

// The hooks that write a package-level variable the brokers read must refuse a
// parallel test: it would write while another parallel test's broker reads it,
// which is a data race (seen in CI as SetSharedAckWait against NewWithContext).
func TestAHookThatWritesASharedVariableRefusesAParallelTest(t *testing.T) {
	t.Run("parallel", func(t *testing.T) {
		t.Parallel()
		defer func() {
			if recover() == nil {
				t.Error("SetSharedAckWait did not refuse a parallel test")
			}
		}()
		SetSharedAckWait(t, time.Second)
	})
}

func TestAHookThatWritesASharedVariableAcceptsASerialTest(t *testing.T) {
	was := sharedAckWait
	t.Cleanup(func() {
		if sharedAckWait != was {
			t.Errorf("not restored: %v", sharedAckWait)
		}
	})
	SetSharedAckWait(t, time.Second)
	if sharedAckWait != time.Second {
		t.Fatalf("sharedAckWait = %v", sharedAckWait)
	}
}
