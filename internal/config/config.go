// Package config loads runtime settings from environment variables and .env.
package config

import (
	"log"
	"math"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// Config holds the server's Redis, gRPC, and rate limit settings.
type Config struct {
	RedisAddr     string
	RedisPassword string
	GRPCAddr      string
	Capacity      int
	RefillRate    float64
}

// Load reads environment settings, loading .env when present.
func Load() *Config {
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		log.Printf("could not load .env: %v", err)
	}

	return &Config{
		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword: getEnv("REDIS_PASSWORD", ""),
		GRPCAddr:      getEnv("GRPC_ADDR", ":50051"),
		Capacity:      getEnvInt("CAPACITY", 10),
		RefillRate:    getEnvFloat("REFILL_RATE", 5.0),
	}
}

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func getEnvInt(key string, def int) int {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		log.Printf("invalid %s=%q, using default %d", key, v, def)
		return def
	}
	return n
}

func getEnvFloat(key string, def float64) float64 {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f <= 0 || math.IsNaN(f) || math.IsInf(f, 0) {
		log.Printf("invalid %s=%q, using default %g", key, v, def)
		return def
	}
	return f
}
