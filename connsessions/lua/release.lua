-- Release a session and free the slot it held. The mirror of admit.lua.
--
-- Idempotent, and that is load-bearing rather than defensive. A session can
-- legitimately be released twice: the client disconnects while the reaper is
-- already cleaning up the same node. If both calls decremented, the count that
-- admission is enforced against would drift below the truth and the org would
-- get capacity nobody paid for.
--
-- DEL on the session hash is the gate. Exactly one caller can observe it as
-- present and delete it, and only that caller touches the counters. In Lua this
-- is safe by construction -- nothing interleaves.
--
-- Counters are floored at zero. A negative count would read as free capacity and
-- effectively disable the limit for that org, which is the worst direction for
-- this bug to fail in.
--
-- The org and node this session belonged to are read from the hash, so the org
-- and node key names are derived inside the script rather than passed in KEYS.
-- ponytail: that makes this script single-instance only -- Redis Cluster requires
-- every touched key to be declared in KEYS so it can verify they share a slot.
-- Upgrade path if this ever moves to Cluster: hash-tag the keys ({org}:...) and
-- pass them explicitly, accepting an extra HMGET round-trip in the caller.
--
-- KEYS[1] session hash    ARGV[1] session_id
-- KEYS[2] org count       ARGV[2] org_slug   (hint; hash wins if present)
-- KEYS[3] global count    ARGV[3] node_id    (hint; hash wins if present)
--
-- Returns {"OK", org_slug, node_id, org_count, global_count}
--      or {"NOOP"} when the session was already gone.

local session_key      = KEYS[1]
local global_count_key = KEYS[3]

local session_id = ARGV[1]

-- Trust the hash over the caller: the reaper knows the node, but a crashed-socket
-- path may know neither, and the hash is what admit.lua actually wrote.
local stored = redis.call('HMGET', session_key, 'org', 'node')
local org_slug = stored[1]
local node_id = stored[2]

if not org_slug then
  org_slug = ARGV[2]
end
if not node_id then
  node_id = ARGV[3]
end

-- Already released, or never existed. Not an error: see the idempotency note.
if redis.call('DEL', session_key) == 0 then
  return {'NOOP'}
end

if not org_slug or org_slug == '' then
  -- Hash was gone but DEL reported a delete: cannot have happened, but if it
  -- somehow does, do not guess at a counter to decrement.
  return {'NOOP'}
end

local org_count_key = 'org:' .. org_slug .. ':count'
local org_set_key = 'org:' .. org_slug .. ':sessions'

redis.call('SREM', org_set_key, session_id)
if node_id and node_id ~= '' then
  redis.call('SREM', 'node:' .. node_id .. ':sessions', session_id)
end

local org_count = redis.call('DECR', org_count_key)
if org_count < 0 then
  redis.call('SET', org_count_key, 0)
  org_count = 0
end

local global_count = redis.call('DECR', global_count_key)
if global_count < 0 then
  redis.call('SET', global_count_key, 0)
  global_count = 0
end

return {'OK', org_slug, node_id or '', org_count, global_count}
