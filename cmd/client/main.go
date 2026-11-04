package main

import (
	"context"
	"flag"
	"log"
	"time"

	pb "github.com/joshua-sajeev/tollgate/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	serverAddr := flag.String("server", "localhost:50051", "gRPC server address")
	userID := flag.String("user-id", "user:001", "user ID whose token bucket to check")
	tokens := flag.Int("tokens", 2, "token cost for this request")
	flag.Parse()
	if *userID == "" {
		log.Fatal("-user-id must not be empty")
	}
	if *tokens <= 0 || int64(*tokens) > 2147483647 {
		log.Fatal("-tokens must be between 1 and 2147483647")
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	r, err := c.CheckLimit(ctx, &pb.CheckLimitRequest{
		UserId: *userID,
		Tokens: int32(*tokens),
	})
	if err != nil {
		log.Fatalf("could not check limit: %v", err)
	}

	log.Printf("Allowed: %v", r.GetAllowed())
	log.Printf("Remaining: %v", r.GetRemaining())
	log.Printf("Retry after: %v seconds", r.GetRetryAfterSeconds())
}
