package backoff

import (
	"context"
	"testing"
	"time"
)

func TestNextDoublesWithJitterUpToMax(t *testing.T) {
	for range 100 {
		b := New()
		step := 5 * time.Second
		for i := range 10 {
			d := b.Next()
			if d < step/2 || d > step {
				t.Fatalf("delay %d = %s, want between %s and %s", i, d, step/2, step)
			}
			step = min(2*step, 5*time.Minute)
		}
		b.Reset()
		if d := b.Next(); d > 5*time.Second {
			t.Fatalf("delay after Reset = %s, want at most 5s", d)
		}
	}
}

// During the 85 min Tibber outage on 2026-10-25, CrashLoopBackOff started the
// exporter 20 times. The in-process backoff costs about as many attempts.
func TestOutageLoad(t *testing.T) {
	for range 100 {
		b := New()
		attempts := 0
		for elapsed := time.Duration(0); elapsed < 85*time.Minute; elapsed += b.Next() {
			attempts++
		}
		if attempts < 20 || attempts > 40 {
			t.Fatalf("%d attempts in 85 min, want 20-40", attempts)
		}
	}
}

func TestSleepStopsWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if Sleep(ctx, time.Hour) {
		t.Fatal("Sleep did not stop when ctx ended")
	}
	if !Sleep(context.Background(), time.Millisecond) {
		t.Fatal("Sleep returned false without ctx ending")
	}
}
