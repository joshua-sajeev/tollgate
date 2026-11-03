# TollGate

A gRPC-based rate limiter backed by Redis, implementing the token bucket algorithm with atomic Lua scripting.

## Status

## File Structure (Planned)

```
├── proto/
│   └── ratelimiter.proto        # service + message definitions
├── gen/                         # protoc-generated Go code (committed or gitignored, your call)
├── server/
│   └── main.go                  # gRPC server, connects to Redis, implements the RPC
├── internal/
│   └── limiter/
│       ├── limiter.go           # token-bucket logic, wraps the Lua script
│       └── limiter.lua          # the atomic Redis script
├── client/
│   └── main.go                  # small CLI that fires requests at the server to prove it works
├── docker-compose.yml           # spins up Redis locally
├── go.mod / go.sum
└── README.md
```

--- 
**Work in progress** — environment setup stage.

This project is being built as a learning exercise.
