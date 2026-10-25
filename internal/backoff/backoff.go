// Package backoff spaces out retries against the Tibber API.
package backoff

import (
	"context"
	"math/rand/v2"
	"time"
)

// Backoff doubles the delay from Base up to Max. Each delay is random
// between half the current step and the full step (equal jitter).
type Backoff struct {
	Base time.Duration
	Max  time.Duration
	step time.Duration
}

// New returns the backoff used for Tibber: 5 s, doubling up to 5 min.
func New() *Backoff {
	return &Backoff{Base: 5 * time.Second, Max: 5 * time.Minute}
}

// Next returns the delay before the next attempt.
func (b *Backoff) Next() time.Duration {
	if b.step == 0 {
		b.step = b.Base
	}
	d := b.step
	b.step = min(2*b.step, b.Max)
	return d/2 + rand.N(d/2+1)
}

// Reset starts again from Base.
func (b *Backoff) Reset() {
	b.step = 0
}

// Sleep waits for d or until ctx ends, and reports whether d passed.
func Sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
