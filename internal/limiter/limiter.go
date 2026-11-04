// Package limiter checks token buckets using an atomic Redis Lua script.
package limiter

import (
	"context"
	_ "embed"
	"fmt"
	"math"
	"strconv"

	"github.com/redis/go-redis/v9"
)

//go:embed limiter.lua
var script string

type Limiter struct {
	redis      *redis.Client
	capacity   int
	refillRate float64
}

// Result contains the decision and bucket state for one request.
type Result struct {
	Allowed           bool
	Remaining         int32
	RetryAfterSeconds int32
}

// New creates a limiter using the given Redis client and bucket settings.
func New(client *redis.Client, capacity int, refillRate float64) (*Limiter, error) {
	if client == nil {
		return nil, fmt.Errorf("redis client must not be nil")
	}
	if capacity <= 0 {
		return nil, fmt.Errorf("capacity must be positive")
	}
	if refillRate <= 0 || math.IsNaN(refillRate) || math.IsInf(refillRate, 0) {
		return nil, fmt.Errorf("refill rate must be positive and finite")
	}
	return &Limiter{redis: client, capacity: capacity, refillRate: refillRate}, nil
}

// Check atomically evaluates and updates the user's token bucket in Redis.
func (l *Limiter) Check(ctx context.Context, userID string, cost int32) (Result, error) {
	if userID == "" {
		return Result{}, fmt.Errorf("user ID must not be empty")
	}
	if cost <= 0 {
		return Result{}, fmt.Errorf("token cost must be positive")
	}

	values, err := l.redis.Eval(ctx, script, []string{"ratelimit:" + userID},
		l.capacity, l.refillRate, cost).Slice()
	if err != nil {
		return Result{}, fmt.Errorf("evaluate rate limit script: %w", err)
	}
	if len(values) != 3 {
		return Result{}, fmt.Errorf("unexpected Lua result length: %d", len(values))
	}

	allowed, err := asFloat(values[0])
	if err != nil {
		return Result{}, fmt.Errorf("parse allowed result: %w", err)
	}
	remaining, err := asFloat(values[1])
	if err != nil {
		return Result{}, fmt.Errorf("parse remaining result: %w", err)
	}
	retryAfter, err := asFloat(values[2])
	if err != nil {
		return Result{}, fmt.Errorf("parse retry-after result: %w", err)
	}

	return Result{
		Allowed:           allowed == 1,
		Remaining:         int32(math.Floor(remaining)),
		RetryAfterSeconds: int32(math.Ceil(retryAfter)),
	}, nil
}

func asFloat(v any) (float64, error) {
	switch n := v.(type) {
	case int64:
		return float64(n), nil
	case string:
		return strconv.ParseFloat(n, 64)
	default:
		return 0, fmt.Errorf("unexpected Lua value type %T", v)
	}
}
