package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	pb "github.com/joshua-sajeev/tollgate/gen"
	"github.com/joshua-sajeev/tollgate/internal/config"
	"github.com/joshua-sajeev/tollgate/internal/limiter"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type server struct {
	pb.UnimplementedRateLimiterServer
	limiter *limiter.Limiter
}

func (s *server) CheckLimit(ctx context.Context, req *pb.CheckLimitRequest) (res *pb.CheckLimitResponse, err error) {
	started := time.Now()
	userID, tokens := req.GetUserId(), req.GetTokens()
	log.Printf("CheckLimit started: user_id=%q tokens=%d", userID, tokens)

	if userID == "" {
		err := status.Error(codes.InvalidArgument, "user_id must not be empty")
		log.Printf("CheckLimit rejected: user_id=%q tokens=%d error=%v elapsed=%s", userID, tokens, err, time.Since(started))
		return nil, err
	}
	if tokens <= 0 {
		err := status.Error(codes.InvalidArgument, "tokens must be positive")
		log.Printf("CheckLimit rejected: user_id=%q tokens=%d error=%v elapsed=%s", userID, tokens, err, time.Since(started))
		return nil, err
	}

	result, err := s.limiter.Check(ctx, userID, tokens)
	if err != nil {
		log.Printf("CheckLimit failed: user_id=%q tokens=%d error=%v elapsed=%s", userID, tokens, err, time.Since(started))
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			return nil, status.Error(codes.DeadlineExceeded, "request timed out")
		case errors.Is(err, context.Canceled):
			return nil, status.Error(codes.Canceled, "request canceled")
		case errors.Is(err, limiter.ErrInternal):
			return nil, status.Error(codes.Internal, "internal error")
		default:
			return nil, status.Error(codes.Unavailable, "service unavailable")
		}
	}
	response := &pb.CheckLimitResponse{
		Allowed:           result.Allowed,
		Remaining:         result.Remaining,
		RetryAfterSeconds: result.RetryAfterSeconds,
	}
	log.Printf("CheckLimit completed: user_id=%q tokens=%d allowed=%t remaining=%d retry_after=%ds elapsed=%s",
		userID, tokens, response.Allowed, response.Remaining, response.RetryAfterSeconds, time.Since(started))
	return response, nil
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
