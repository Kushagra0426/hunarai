-- Release every session belonging to a node that has stopped heartbeating.
--
-- Requirement 2: when a node crashes, nobody sends a goodbye. The sessions it
-- held are gone but still counted, and an org whose quota is full of ghosts
-- cannot connect. This is what corrects that.
--
-- Atomic for the same reason admit.lua is: the decrements and the set removals
-- must not interleave with a live release of the same session. A client
-- disconnecting at the moment its node is declared dead is the normal case, not
-- an exotic one, and both paths would otherwise decrement for one session.
--
-- Each session is deleted with DEL acting as the gate, exactly as in
-- release.lua: if the hash is already gone the session was released by its own
-- consumer and this loop must not touch the counters for it.
--
-- Counts are accumulated per-org in a Lua table and applied once at the end,
-- rather than one DECR per session. A node holding a thousand sessions is one
-- DECRBY per org instead of a thousand DECRs.
--
-- ponytail: reaps a node's whole set in one script. A node holding tens of
-- thousands of sessions would block Redis for the duration, since scripts are
-- single-threaded. Upgrade path: pass a batch size, SPOP that many per call, and
-- let the caller loop until the set is empty.
--
-- KEYS[1] node session set
-- KEYS[2] global count
-- KEYS[3] nodes:alive sorted set
-- ARGV[1] node_id
--
-- Returns {reaped_count, org_slug, count, org_slug, count, ...}

local node_set_key = KEYS[1]
local global_count_key = KEYS[2]
local alive_key = KEYS[3]
local node_id = ARGV[1]

local session_ids = redis.call('SMEMBERS', node_set_key)
local per_org = {}
local order = {}
local reaped = 0

for i = 1, #session_ids do
  local sid = session_ids[i]
  local session_key = 'session:' .. sid
  local org_slug = redis.call('HGET', session_key, 'org')

  -- DEL is the gate. Zero means the consumer already released this session, so
  -- its slot has been accounted for and must not be counted again.
  if redis.call('DEL', session_key) == 1 and org_slug then
    redis.call('SREM', 'org:' .. org_slug .. ':sessions', sid)
    if per_org[org_slug] == nil then
      per_org[org_slug] = 0
      order[#order + 1] = org_slug
    end
    per_org[org_slug] = per_org[org_slug] + 1
    reaped = reaped + 1
  end
end

-- Apply the totals. Floored at zero: a negative count would read as free
-- capacity and effectively disable the org's limit.
local result = {reaped}
for i = 1, #order do
  local org_slug = order[i]
  local n = per_org[org_slug]
  local org_count_key = 'org:' .. org_slug .. ':count'
  local remaining = redis.call('DECRBY', org_count_key, n)
  if remaining < 0 then
    redis.call('SET', org_count_key, 0)
  end
  result[#result + 1] = org_slug
  result[#result + 1] = n
end

if reaped > 0 then
  local global_remaining = redis.call('DECRBY', global_count_key, reaped)
  if global_remaining < 0 then
    redis.call('SET', global_count_key, 0)
  end
end

-- The node is gone: drop its now-empty session set, its heartbeat marker and its
-- membership of the liveness set. Removing it from nodes:alive is what stops a
-- second reaper from finding and reaping it again.
redis.call('DEL', node_set_key)
redis.call('DEL', 'node:' .. node_id .. ':hb')
redis.call('DEL', 'node:' .. node_id .. ':draining')
redis.call('ZREM', alive_key, node_id)

return result
