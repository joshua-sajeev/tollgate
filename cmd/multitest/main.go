// Command multitest fires concurrent CheckLimit requests at two separate
// RateLimiter server instances for the same user, proving they share a
// single global token bucket via Redis rather than enforcing the limit
// independently per instance.
package main

import (
	"context"
	"flag"
	"log"
	"sync"
	"time"

	pb "github.com/joshua-sajeev/tollgate/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr1 := flag.String("addr1", "localhost:50051", "address of server instance 1")
	addr2 := flag.String("addr2", "localhost:50052", "address of server instance 2")
	userID := flag.String("user", "multitest-user", "user id shared across both servers")
	capacity := flag.Int("capacity", 10, "expected bucket capacity (must match server config)")
	requestsPerServer := flag.Int("per-server", 15, "number of concurrent requests to send to each server")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn1 := mustDial(ctx, *addr1)
	defer conn1.Close()
	conn2 := mustDial(ctx, *addr2)
	defer conn2.Close()

	client1 := pb.NewRateLimiterClient(conn1)
	client2 := pb.NewRateLimiterClient(conn2)

	var wg sync.WaitGroup
	var mu sync.Mutex
	allowedCount := 0
	deniedCount := 0
	errCount := 0

	start := make(chan struct{})

	fire := func(client pb.RateLimiterClient, serverLabel string, n int) {
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(reqNum int) {
				defer wg.Done()
				<-start

				res, err := client.CheckLimit(ctx, &pb.CheckLimitRequest{
					UserId: *userID,
					Tokens: 1,
				})

				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					errCount++
					log.Printf("[%s] request %d error: %v", serverLabel, reqNum, err)
					return
				}
				if res.GetAllowed() {
					allowedCount++
					log.Printf("[%s] request %d: ALLOWED (remaining=%d)", serverLabel, reqNum, res.GetRemaining())
				} else {
					deniedCount++
					log.Printf("[%s] request %d: DENIED (retry_after=%ds)", serverLabel, reqNum, res.GetRetryAfterSeconds())
				}
			}(i)
		}
	}

	fire(client1, "server-1", *requestsPerServer)
	fire(client2, "server-2", *requestsPerServer)

	close(start) // release all goroutines to fire as close to simultaneously as possible
	wg.Wait()

	total := *requestsPerServer * 2
	log.Printf("---")
	log.Printf("total requests sent: %d (across 2 server instances)", total)
	log.Printf("allowed: %d | denied: %d | errors: %d", allowedCount, deniedCount, errCount)

	if errCount > 0 {
		log.Printf("FAIL: %d requests errored, cannot validate correctness", errCount)
		return
	}
	if allowedCount == *capacity {
		log.Printf("PASS: exactly %d requests allowed across both instances, matching configured capacity", *capacity)
	} else {
		log.Printf("FAIL: expected exactly %d allowed, got %d — instances are NOT sharing global state correctly", *capacity, allowedCount)
	}
}

func mustDial(ctx context.Context, addr string) *grpc.ClientConn {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to dial %s: %v", addr, err)
	}
	return conn
}
