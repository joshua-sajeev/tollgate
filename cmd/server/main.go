package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"

	pb "github.com/joshua-sajeev/tollgate/gen"
	"github.com/joshua-sajeev/tollgate/internal/config"
	"github.com/joshua-sajeev/tollgate/internal/limiter"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedRateLimiterServer
	limiter *limiter.Limiter
}

func (s *server) CheckLimit(ctx context.Context, req *pb.CheckLimitRequest) (res *pb.CheckLimitResponse, err error) {
	result, err := s.limiter.Check(ctx, req.GetUserId(), req.GetTokens())
	if err != nil {
		return nil, err
	}
	return &pb.CheckLimitResponse{
		Allowed:           result.Allowed,
		Remaining:         result.Remaining,
		RetryAfterSeconds: result.RetryAfterSeconds,
	}, nil
}

func main() {
	if err := run(); err != nil {
		log.Printf("server stopped: %v", err)
		os.Exit(1)
	}
}

func run() (err error) {
	cfg := config.Load()
	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
	})

	defer func() {
		if closeErr := redisClient.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close Redis client: %w", closeErr))
		}
	}()

	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		return fmt.Errorf("connect to Redis at %s: %w", cfg.RedisAddr, err)
	}

	rateLimiter, err := limiter.New(redisClient, cfg.Capacity, cfg.RefillRate)
	if err != nil {
		return fmt.Errorf("invalid rate limiter configuration: %w", err)
	}
	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.GRPCAddr, err)
	}
	defer func() {
		if closeErr := lis.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close listener: %w", closeErr))
		}
	}()

	s := grpc.NewServer()
	pb.RegisterRateLimiterServer(s, &server{limiter: rateLimiter})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		return fmt.Errorf("serve gRPC: %w", err)
	}
	return nil
}
