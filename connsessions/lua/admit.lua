-- Admission control. The correctness core of the whole system.
--
-- Why Lua: the check and the increment must not interleave. Two connections
-- arriving in the same millisecond on different nodes would otherwise both read
-- "499 of 500", both decide there is room, and both be admitted. Redis runs this
-- script start to finish without another client's commands in between, so the
-- two get serialised: one admitted, one rejected.
--
-- Everything the decision needs is read and written here. A node never decides
-- admission from its own local view, because it cannot see the other nodes.
--
-- KEYS[1] org count      ARGV[1] session_id      ARGV[5] org_max
-- KEYS[2] global count   ARGV[2] org_slug        ARGV[6] reserved_floor
-- KEYS[3] org set        ARGV[3] node_id         ARGV[7] global_max
-- KEYS[4] node set       ARGV[4] client_id       ARGV[8] started_at
-- KEYS[5] session hash
-- KEYS[6] node draining flag
--
-- Returns {"OK", org_count, global_count}
--      or {"REJECTED", reason, org_count, global_count}

local org_count_key   = KEYS[1]
local global_count_key = KEYS[2]
local org_set_key     = KEYS[3]
local node_set_key    = KEYS[4]
local session_key     = KEYS[5]
local draining_key    = KEYS[6]

local session_id   = ARGV[1]
local org_slug     = ARGV[2]
local node_id      = ARGV[3]
local client_id    = ARGV[4]
local org_max      = tonumber(ARGV[5])
local floor        = tonumber(ARGV[6])
local global_max   = tonumber(ARGV[7])
local started_at   = ARGV[8]

-- GET returns a Lua string (or false when absent), never a number, so coerce
-- explicitly. Comparing a string to a number in Lua is always false, which
-- would silently admit everything.
local org_count    = tonumber(redis.call('GET', org_count_key)) or 0
local global_count = tonumber(redis.call('GET', global_count_key)) or 0

-- Reject a re-registration of a live session id outright. Without this a retry
-- would increment the counters twice for one connection and leak a slot.
if redis.call('EXISTS', session_key) == 1 then
  return {'REJECTED', 'DUPLICATE_SESSION', org_count, global_count}
end

-- A draining node must take no new work. Checked here rather than in Python so
-- the decision is atomic with the increment: a node that starts draining
-- mid-handshake cannot still pick up the session.
if redis.call('EXISTS', draining_key) == 1 then
  return {'REJECTED', 'NODE_DRAINING', org_count, global_count}
end

-- 1. The org's own ceiling. What the tenant pays for, and it binds regardless of
--    how empty the platform is.
if org_count >= org_max then
  return {'REJECTED', 'ORG_LIMIT', org_count, global_count}
end

-- 2. Fair sharing. The global pool is shared, so an org that has already reached
--    its reserved floor competes first-come for what is left and loses when the
--    platform is full. Below its floor it is admitted anyway: that capacity is
--    guaranteed, and it is what stops one noisy tenant from locking out a paying
--    one that is well under its own limit.
if global_count >= global_max and org_count >= floor then
  return {'REJECTED', 'GLOBAL_FULL', org_count, global_count}
end

-- Admitted. Counters and both indexes move together, so a session can never be
-- counted without being findable, or findable without being counted -- either
-- would break reaping or the query API.
redis.call('HSET', session_key,
  'session_id', session_id,
  'org', org_slug,
  'node', node_id,
  'client', client_id,
  'started_at', started_at)
redis.call('SADD', org_set_key, session_id)
redis.call('SADD', node_set_key, session_id)
org_count = redis.call('INCR', org_count_key)
global_count = redis.call('INCR', global_count_key)

return {'OK', org_count, global_count}
