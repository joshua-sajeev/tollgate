-- limiter.lua
-- KEYS[1] = bucket key, e.g. "ratelimit:{identity}"
-- ARGV[1] = capacity      (max tokens the bucket can hold)
-- ARGV[2] = refill_rate   (tokens added per second)
-- ARGV[3] = cost          (tokens this request needs)

local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])

-- Server time, not client time — keeps every instance on one clock.
-- TIME returns {seconds, microseconds} as strings.
local t = redis.call("TIME")
local now = tonumber(t[1]) + (tonumber(t[2]) / 1000000)

-- Read existing state (hash with two fields).
local bucket = redis.call("HMGET", key, "tokens", "last_refill")
local tokens = tonumber(bucket[1])
local last_refill = tonumber(bucket[2])

-- Key doesn't exist yet: first request for this identity starts full.
if tokens == nil then
	tokens = capacity
	last_refill = now
end

-- Refill, capped at capacity. Guard against negative elapsed time
-- in case of clock weirdness between calls.
local elapsed = now - last_refill
if elapsed < 0 then
	elapsed = 0
end

local refilled = tokens + (elapsed * refill_rate)
if refilled > capacity then
	refilled = capacity
end

local allowed
local remaining
local retry_after

if refilled >= cost then
	remaining = refilled - cost
	allowed = 1
	retry_after = 0
else
	-- Deny, but the refill that happened is still real — persist it
	-- instead of leaving the bucket at its old, stale balance.
	remaining = refilled
	allowed = 0
	retry_after = (cost - refilled) / refill_rate
end

redis.call("HMSET", key, "tokens", remaining, "last_refill", now)

-- Let idle buckets expire instead of living in Redis forever:
-- time to go from empty to full, plus a small safety margin.
local ttl = math.ceil(capacity / refill_rate) + 10
redis.call("EXPIRE", key, ttl)

return { allowed, tostring(remaining), tostring(retry_after) }
