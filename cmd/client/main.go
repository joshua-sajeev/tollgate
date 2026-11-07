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
	serverAddr := flag.String("server", "localhost:50051", "gRPC server address")
	userID := flag.String("user-id", "user:001", "user ID whose token bucket to check")
	tokens := flag.Int("tokens", 2, "token cost for this request")
	n := flag.Int("n", 1, "number of concurrent requests to fire")
	timeout := flag.Duration("timeout", 2*time.Second, "maximum time to wait for each request (for example, 10s or 1m)")
	flag.Parse()

	provided := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) {
		provided[f.Name] = true
	})
	for _, name := range []string{"user-id", "tokens", "n"} {
		if !provided[name] {
			log.Fatalf("-%s is required", name)
		}
	}
	if *userID == "" {
		log.Fatal("-user-id must not be empty")
	}
	if *tokens <= 0 || int64(*tokens) > 2147483647 {
		log.Fatal("-tokens must be between 1 and 2147483647")
	}
	if *n <= 0 {
		log.Fatal("-n must be at least 1")
	}
	if *timeout <= 0 {
		log.Fatal("-timeout must be greater than zero")
	}

	conn, err := grpc.NewClient(
		*serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("failed to create client connection: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("failed to close connection: %v", err)
		}
	}()

	c := pb.NewRateLimiterClient(conn)

	var wg sync.WaitGroup
	for i := range *n {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), *timeout)
			defer cancel()

			r, err := c.CheckLimit(ctx, &pb.CheckLimitRequest{
				UserId: *userID,
				Tokens: int32(*tokens),
			})
			if err != nil {
				log.Printf("[%2d] error: %v", i, err)
				return
			}

			status := "DENIED"
			if r.GetAllowed() {
				status = "ALLOWED"
			}

			log.Printf("[%2d] %-7s remaining=%-4d retry_after=%ds",
				i, status, r.GetRemaining(), r.GetRetryAfterSeconds())
		})
	}

	wg.Wait()
}
