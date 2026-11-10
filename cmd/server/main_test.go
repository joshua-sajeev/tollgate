package main

import (
	"context"
	"errors"
	"fmt"
	"testing"

	pb "github.com/joshua-sajeev/tollgate/gen"
	"github.com/joshua-sajeev/tollgate/internal/limiter"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeChecker struct {
	result  limiter.Result
	err     error
	gotID   string
	gotCost int32
}

func (f *fakeChecker) Check(ctx context.Context, userID string, cost int32) (limiter.Result, error) {
	f.gotID = userID
	f.gotCost = cost
	return f.result, f.err
}

func TestServer_CheckLimit_Success(t *testing.T) {
	fake := &fakeChecker{
		result: limiter.Result{
			Allowed:           true,
			Remaining:         9,
			RetryAfterSeconds: 0,
		},
	}
	s := &server{limiter: fake}

	resp, err := s.CheckLimit(context.Background(), &pb.CheckLimitRequest{
		UserId: "user-123",
		Tokens: 2,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if fake.gotID != "user-123" || fake.gotCost != 2 {
		t.Errorf("checker received ID %q cost %d, want 'user-123' and 2", fake.gotID, fake.gotCost)
	}
	if !resp.Allowed {
		t.Errorf("Allowed = false, want true")
	}
	if resp.Remaining != 9 {
		t.Errorf("Remaining = %d, want 9", resp.Remaining)
	}
	if resp.RetryAfterSeconds != 0 {
		t.Errorf("RetryAfterSeconds = %d, want 0", resp.RetryAfterSeconds)
	}
}

func TestServer_CheckLimit_Denied(t *testing.T) {
	fake := &fakeChecker{
		result: limiter.Result{
			Allowed:           false,
			Remaining:         0,
			RetryAfterSeconds: 5,
		},
	}
	s := &server{limiter: fake}

	resp, err := s.CheckLimit(context.Background(), &pb.CheckLimitRequest{
		UserId: "user-123",
		Tokens: 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Allowed {
		t.Errorf("Allowed = true, want false")
	}
	if resp.Remaining != 0 {
		t.Errorf("Remaining = %d, want 0", resp.Remaining)
	}
	if resp.RetryAfterSeconds != 5 {
		t.Errorf("RetryAfterSeconds = %d, want 5", resp.RetryAfterSeconds)
	}
}

func TestServer_CheckLimit_ValidationErrors(t *testing.T) {
	s := &server{limiter: &fakeChecker{}}

	tests := []struct {
		name    string
		req     *pb.CheckLimitRequest
		wantMsg string
	}{
		{
			name:    "empty user_id",
			req:     &pb.CheckLimitRequest{UserId: "", Tokens: 1},
			wantMsg: "user_id must not be empty",
		},
		{
			name:    "zero tokens",
			req:     &pb.CheckLimitRequest{UserId: "user1", Tokens: 0},
			wantMsg: "tokens must be positive",
		},
		{
			name:    "negative tokens",
			req:     &pb.CheckLimitRequest{UserId: "user1", Tokens: -5},
			wantMsg: "tokens must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := s.CheckLimit(context.Background(), tt.req)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			st, ok := status.FromError(err)
			if !ok {
				t.Fatalf("expected grpc status error, got %v", err)
			}
			if st.Code() != codes.InvalidArgument {
				t.Errorf("code = %v, want %v", st.Code(), codes.InvalidArgument)
			}
			if st.Message() != tt.wantMsg {
				t.Errorf("message = %q, want %q", st.Message(), tt.wantMsg)
			}
		})
	}
}

func TestServer_CheckLimit_ErrorMapping(t *testing.T) {
	tests := []struct {
		name     string
		checkErr error
		wantCode codes.Code
		wantMsg  string
	}{
		{
			name:     "deadline exceeded",
			checkErr: context.DeadlineExceeded,
			wantCode: codes.DeadlineExceeded,
			wantMsg:  "request timed out",
		},
		{
			name:     "context canceled",
			checkErr: context.Canceled,
			wantCode: codes.Canceled,
			wantMsg:  "request canceled",
		},
		{
			name:     "internal limiter error",
			checkErr: fmt.Errorf("lua failure: %w", limiter.ErrInternal),
			wantCode: codes.Internal,
			wantMsg:  "internal error",
		},
		{
			name:     "generic / redis error",
			checkErr: errors.New("dial tcp 127.0.0.1:6379: connect: connection refused"),
			wantCode: codes.Unavailable,
			wantMsg:  "service unavailable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeChecker{err: tt.checkErr}
			s := &server{limiter: fake}

			_, err := s.CheckLimit(context.Background(), &pb.CheckLimitRequest{
				UserId: "user1",
				Tokens: 1,
			})
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			st, ok := status.FromError(err)
			if !ok {
				t.Fatalf("expected grpc status error, got %v", err)
			}
			if st.Code() != tt.wantCode {
				t.Errorf("code = %v, want %v", st.Code(), tt.wantCode)
			}
			if st.Message() != tt.wantMsg {
				t.Errorf("message = %q, want %q", st.Message(), tt.wantMsg)
			}
		})
	}
}
