package main

import (
	"context"
	"log"
	"time"

	pb "github.com/joshua-sajeev/tollgate/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient(
		"localhost:50051",
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
		UserId: "user:001",
		Tokens: 2,
	})
	if err != nil {
		log.Fatalf("could not check limit: %v", err)
	}

	log.Printf("Allowed: %v", r.GetAllowed())
	log.Printf("Remaining: %v", r.GetRemaining())
	log.Printf("Retry after: %v seconds", r.GetRetryAfterSeconds())
}
