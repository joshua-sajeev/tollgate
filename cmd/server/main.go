package main

import (
	"context"
	"log"
	"net"

	pb "github.com/joshua-sajeev/tollgate/gen"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedRateLimiterServer
}

func (s *server) CheckLimit(ctx context.Context, req *pb.CheckLimitRequest) (res *pb.CheckLimitResponse, err error) {
	return &pb.CheckLimitResponse{Allowed: true, Remaining: 10, RetryAfterSeconds: 0}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterRateLimiterServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
