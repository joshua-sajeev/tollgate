package limiter

import (
	"context"
	"errors"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func newTestClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	client := redis.NewClient(&redis.Options{Addr: addr})

	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Skipf("redis not available at %s, skipping: %v", addr, err)
	}
	return client
}

func flushKey(t *testing.T, client *redis.Client, key string) {
	t.Helper()
	if err := client.Del(context.Background(), "ratelimit:"+key).Err(); err != nil {
		t.Fatalf("failed to flush key: %v", err)
	}
}

func TestNew(t *testing.T) {
	client := newTestClient(t)

	tests := []struct {
		name       string
		client     *redis.Client
		capacity   int
		refillRate float64
		wantErr    bool
	}{
		{name: "valid", client: client, capacity: 10, refillRate: 1.0, wantErr: false},
		{name: "nil client", client: nil, capacity: 10, refillRate: 1.0, wantErr: true},
		{name: "zero capacity", client: client, capacity: 0, refillRate: 1.0, wantErr: true},
		{name: "negative capacity", client: client, capacity: -5, refillRate: 1.0, wantErr: true},
		{name: "zero refill rate", client: client, capacity: 10, refillRate: 0, wantErr: true},
		{name: "negative refill rate", client: client, capacity: 10, refillRate: -1.0, wantErr: true},
		{name: "NaN refill rate", client: client, capacity: 10, refillRate: math.NaN(), wantErr: true},
		{name: "positive Inf refill rate", client: client, capacity: 10, refillRate: math.Inf(1), wantErr: true},
		{name: "negative Inf refill rate", client: client, capacity: 10, refillRate: math.Inf(-1), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.client, tt.capacity, tt.refillRate)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCheck_InputValidation(t *testing.T) {
	client := newTestClient(t)
	l, err := New(client, 10, 1.0)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	tests := []struct {
		name    string
		userID  string
		cost    int32
		wantErr bool
	}{
		{name: "valid", userID: "user1", cost: 1, wantErr: false},
		{name: "empty userID", userID: "", cost: 1, wantErr: true},
		{name: "zero cost", userID: "user1", cost: 0, wantErr: true},
		{name: "negative cost", userID: "user1", cost: -1, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := l.Check(context.Background(), tt.userID, tt.cost)
			if (err != nil) != tt.wantErr {
				t.Errorf("Check() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCheck_Behavior(t *testing.T) {
	client := newTestClient(t)

	tests := []struct {
		name          string
		key           string
		capacity      int
		refillRate    float64
		requestsCost  []int32 // sequence of costs to send before the assertion
		checkCost     int32   // cost of final check; defaults to 1 if <= 0
		wantAllowed   bool    // result of the FINAL request after the sequence
		wantRemaining int32   // -1 to skip this check
		sleepBefore   time.Duration
	}{
		{
			name:          "single request within capacity allowed",
			key:           "test-single-within",
			capacity:      5,
			refillRate:    1.0,
			requestsCost:  nil,
			checkCost:     1,
			wantAllowed:   true,
			wantRemaining: 4,
		},
		{
			name:          "multi-token request within capacity allowed",
			key:           "test-multi-token-within",
			capacity:      10,
			refillRate:    1.0,
			requestsCost:  nil,
			checkCost:     4,
			wantAllowed:   true,
			wantRemaining: 6,
		},
		{
			name:          "request exactly draining capacity allowed",
			key:           "test-drain-exact",
			capacity:      3,
			refillRate:    1.0,
			requestsCost:  []int32{1, 1}, // 2 prior requests, 3rd drains it
			checkCost:     1,
			wantAllowed:   true,
			wantRemaining: 0,
		},
		{
			name:          "multi-token request exactly draining capacity allowed",
			key:           "test-multi-drain-exact",
			capacity:      6,
			refillRate:    1.0,
			requestsCost:  []int32{2},
			checkCost:     4,
			wantAllowed:   true,
			wantRemaining: 0,
		},
		{
			name:          "request beyond capacity denied",
			key:           "test-beyond-capacity",
			capacity:      3,
			refillRate:    1.0,
			requestsCost:  []int32{1, 1, 1}, // drains bucket
			checkCost:     1,
			wantAllowed:   false,
			wantRemaining: -1,
		},
		{
			name:          "multi-token request exceeding remaining balance denied",
			key:           "test-multi-exceed-balance",
			capacity:      5,
			refillRate:    1.0,
			requestsCost:  []int32{3},
			checkCost:     3,
			wantAllowed:   false,
			wantRemaining: -1,
		},
		{
			name:          "request with cost exceeding total bucket capacity denied",
			key:           "test-cost-exceeds-capacity",
			capacity:      5,
			refillRate:    1.0,
			requestsCost:  nil,
			checkCost:     10,
			wantAllowed:   false,
			wantRemaining: -1,
		},
		{
			name:          "request allowed after refill wait",
			key:           "test-refill-wait",
			capacity:      2,
			refillRate:    10.0,          // fast refill for test speed
			requestsCost:  []int32{1, 1}, // drains bucket
			checkCost:     1,
			sleepBefore:   200 * time.Millisecond,
			wantAllowed:   true,
			wantRemaining: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flushKey(t, client, tt.key)

			l, err := New(client, tt.capacity, tt.refillRate)
			if err != nil {
				t.Fatalf("New() failed: %v", err)
			}

			ctx := context.Background()

			for i, cost := range tt.requestsCost {
				if _, err := l.Check(ctx, tt.key, cost); err != nil {
					t.Fatalf("setup request %d failed: %v", i, err)
				}
			}

			if tt.sleepBefore > 0 {
				time.Sleep(tt.sleepBefore)
			}

			cost := tt.checkCost
			if cost <= 0 {
				cost = 1
			}

			res, err := l.Check(ctx, tt.key, cost)
			if err != nil {
				t.Fatalf("Check() failed: %v", err)
			}
			if res.Allowed != tt.wantAllowed {
				t.Errorf("Allowed = %v, want %v", res.Allowed, tt.wantAllowed)
			}
			if tt.wantRemaining >= 0 && res.Remaining != tt.wantRemaining {
				t.Errorf("Remaining = %d, want %d", res.Remaining, tt.wantRemaining)
			}
			if !tt.wantAllowed && res.RetryAfterSeconds <= 0 {
				t.Errorf("expected positive RetryAfterSeconds when denied, got %d", res.RetryAfterSeconds)
			}
		})
	}
}

// TestCheck_Concurrent proves the Lua script's atomicity holds under
// concurrent access: N goroutines hammering the same key simultaneously
// should allow exactly `capacity` requests through, no more.
func TestCheck_Concurrent(t *testing.T) {
	client := newTestClient(t)

	tests := []struct {
		name        string
		capacity    int
		numRequests int
	}{
		{name: "burst of 50 against capacity 10", capacity: 10, numRequests: 50},
		{name: "burst of 20 against capacity 1", capacity: 1, numRequests: 20},
		{name: "burst matches capacity exactly", capacity: 15, numRequests: 15},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := "test-concurrent-" + tt.name
			flushKey(t, client, key)

			l, err := New(client, tt.capacity, 1.0) // slow refill, irrelevant during burst
			if err != nil {
				t.Fatalf("New() failed: %v", err)
			}

			var wg sync.WaitGroup
			var mu sync.Mutex
			allowedCount := 0
			start := make(chan struct{})

			for i := 0; i < tt.numRequests; i++ {
				wg.Go(func() {
					<-start

					res, err := l.Check(context.Background(), key, 1)
					if err != nil {
						t.Errorf("Check() failed: %v", err)
						return
					}
					if res.Allowed {
						mu.Lock()
						allowedCount++
						mu.Unlock()
					}
				})
			}

			close(start)
			wg.Wait()

			if allowedCount != tt.capacity {
				t.Errorf("allowed = %d, want exactly %d", allowedCount, tt.capacity)
			}
		})
	}
}

// TestCheck_ConcurrentIsolation verifies concurrent requests against
// different keys don't interfere with each other's bucket state.
func TestCheck_ConcurrentIsolation(t *testing.T) {
	client := newTestClient(t)
	const capacity = 5
	const overshoot = 5

	l, err := New(client, capacity, 1.0)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	keys := []string{"test-iso-a", "test-iso-b", "test-iso-c"}
	for _, k := range keys {
		flushKey(t, client, k)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	results := make(map[string]int)

	for _, key := range keys {
		for range capacity + overshoot {
			wg.Add(1)
			go func(k string) {
				defer wg.Done()
				res, err := l.Check(context.Background(), k, 1)
				if err != nil {
					t.Errorf("Check() failed: %v", err)
					return
				}
				if res.Allowed {
					mu.Lock()
					results[k]++
					mu.Unlock()
				}
			}(key)
		}
	}

	wg.Wait()

	for _, key := range keys {
		if results[key] != capacity {
			t.Errorf("key %q: allowed = %d, want %d", key, results[key], capacity)
		}
	}
}

func TestCheck_TTL(t *testing.T) {
	client := newTestClient(t)
	key := "test-ttl"
	flushKey(t, client, key)

	capacity := 10
	refillRate := 2.0
	l, err := New(client, capacity, refillRate)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()
	if _, err := l.Check(ctx, key, 1); err != nil {
		t.Fatalf("Check() failed: %v", err)
	}

	ttl, err := client.TTL(ctx, "ratelimit:"+key).Result()
	if err != nil {
		t.Fatalf("client.TTL() failed: %v", err)
	}

	// Lua script sets ttl = ceil(capacity / refill_rate) + 10
	// For cap 10, rate 2: 5 + 10 = 15 seconds
	expectedTTLSeconds := int64(math.Ceil(float64(capacity)/refillRate)) + 10
	if ttl <= 0 || ttl > time.Duration(expectedTTLSeconds)*time.Second {
		t.Errorf("got TTL %v, want > 0 and <= %ds", ttl, expectedTTLSeconds)
	}
}

func TestCheck_Context(t *testing.T) {
	client := newTestClient(t)
	l, err := New(client, 10, 1.0)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("canceled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := l.Check(ctx, "test-ctx-canceled", 1)
		if err == nil {
			t.Fatal("expected error for canceled context, got nil")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})

	t.Run("deadline exceeded context", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
		defer cancel()

		_, err := l.Check(ctx, "test-ctx-deadline", 1)
		if err == nil {
			t.Fatal("expected error for deadline exceeded context, got nil")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected context.DeadlineExceeded, got %v", err)
		}
	})
}

func TestCheck_RedisFailure(t *testing.T) {
	badClient := redis.NewClient(&redis.Options{
		Addr:        "localhost:1",
		DialTimeout: 50 * time.Millisecond,
	})
	defer badClient.Close()

	l, err := New(badClient, 10, 1.0)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err = l.Check(ctx, "test-redis-fail", 1)
	if err == nil {
		t.Fatal("expected error when Redis unavailable, got nil")
	}
}

func Test_asFloat(t *testing.T) {
	tests := []struct {
		name    string
		input   any
		want    float64
		wantErr bool
	}{
		{name: "int64", input: int64(42), want: 42.0, wantErr: false},
		{name: "string valid float", input: "3.14", want: 3.14, wantErr: false},
		{name: "string invalid", input: "not-a-number", want: 0, wantErr: true},
		{name: "unsupported bool type", input: true, want: 0, wantErr: true},
		{name: "unsupported struct type", input: struct{}{}, want: 0, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := asFloat(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("asFloat(%v) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("asFloat(%v) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}
