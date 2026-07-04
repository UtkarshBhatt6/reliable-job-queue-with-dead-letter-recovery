package queue

import (
	"math"
	"math/rand"
	"time"
)

// RetryPolicy defines the parameters for computing retry intervals.
type RetryPolicy struct {
	BaseDelay  time.Duration
	MaxDelay   time.Duration
	Multiplier float64
	Jitter     bool
}

// DefaultRetryPolicy returns a standard retry policy with exponential backoff and jitter.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		BaseDelay:  1 * time.Second,
		MaxDelay:   5 * time.Minute,
		Multiplier: 2.0,
		Jitter:     true,
	}
}

// CalculateBackoff computes the duration to wait before retrying a job.
func (p RetryPolicy) CalculateBackoff(retries int) time.Duration {
	base := p.BaseDelay
	if base <= 0 {
		base = 1 * time.Second
	}
	
	multiplier := p.Multiplier
	if multiplier <= 0 {
		multiplier = 2.0
	}
	
	maxDelay := p.MaxDelay
	if maxDelay <= 0 {
		maxDelay = 5 * time.Minute
	}

	// Calculate: base * (multiplier ^ retries)
	backoffFloat := float64(base) * math.Pow(multiplier, float64(retries))
	
	// Check overflow
	var delay time.Duration
	if backoffFloat > float64(maxDelay) {
		delay = maxDelay
	} else {
		delay = time.Duration(backoffFloat)
	}

	if p.Jitter {
		// Seed local source to avoid global lock
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		// Half jitter: wait base delay + randomized offset up to half delay
		halfDelay := int64(delay / 2)
		if halfDelay > 0 {
			delay = time.Duration(halfDelay + rng.Int63n(halfDelay))
		}
	}

	return delay
}
