package vercelworld

// Every mutation is one Lua script, so it is atomic over TCP and REST. Scripts
// receive no KEYS. ARGV[1] is the key prefix and keys are derived as in
// world_storage.go. Binary payloads are base64 text built in Go, and Go passes
// any field parsed from event data in a JSON descriptor, so Lua only decodes
// JSON that Go produced. Timestamps are Unix microseconds passed from Go.
// Results are arrays whose first element is "ok" or "err" followed by a code.

const luaPrelude = `
local P = ARGV[1]
local function hex(s) return (string.gsub(s, '.', function(c) return string.format('%02x', string.byte(c)) end)) end
local function k(kind, id) return P .. ':' .. kind .. ':' .. id end
local function terminal(s) return s == 'completed' or s == 'failed' or s == 'cancelled' end
local function fmt(n) return string.format('%.0f', n) end
local function nextScore(now)
  local last = tonumber(redis.call('GET', P .. ':order') or '0')
  local score = now
  if score <= last then score = last + 1 end
  redis.call('SET', P .. ':order', fmt(score))
  return score
end
local function rpush(key, values)
  for i = 1, #values, 1000 do
    redis.call('RPUSH', key, unpack(values, i, math.min(i + 999, #values)))
  end
end
`

// scriptCreateRun takes ARGV prefix, runID, now, workflowName, record,
// deploymentID, and attribute key/value pairs... The run's input stays in its
// run_created record at slot 1.
var scriptCreateRun = newRedisScript(luaPrelude + `
local id, now, wf = ARGV[2], ARGV[3], ARGV[4]
local key = k('run', id)
if redis.call('EXISTS', key) == 1 then return {'err', 'run_exists'} end
local score = fmt(nextScore(tonumber(now)))
redis.call('HSET', key, 'wf', wf, 'status', 'pending', 'createdAt', now, 'updatedAt', now, 'count', '1', 'score', score)
if ARGV[6] ~= '' then redis.call('HSET', key, 'deployment', ARGV[6]) end
redis.call('RPUSH', k('events', id), ARGV[5])
if #ARGV > 6 then redis.call('HSET', k('attrs', id), unpack(ARGV, 7)) end
redis.call('ZADD', P .. ':runs', score, id)
redis.call('ZADD', P .. ':runs:wf:' .. wf, score, id)
redis.call('ZADD', P .. ':runs:status:pending', score, id)
redis.call('ZADD', P .. ':runs:wfstatus:pending:' .. wf, score, id)
return {'ok'}
`)

// scriptAppend admits and appends events. Its ARGV are prefix, runID, now, mode
// (append|batch|claim), delta start index (-1 for none), force flag, in-band
// count (empty for an out-of-band write), then one JSON descriptor per event (see
// eventDescriptor). In claim mode the last ARGV is the base64 data of the
// hook_disposed a forced claim appends. Changes are staged in memory and
// written only after every event is admitted, so a rejected batch writes
// nothing. Views keep the slots of the events that carry their payloads
// rather than copies, so the event log is the only copy. An in-band count that
// is not the run's 'inband' field rejects the write with in_band_superseded,
// and an accepted in-band write advances it.
//
// append and claim return {'ok', slot, type, allocated, step, forcedRunID,
// deltaStart, inBandCount, deltaRecords...}, while batch returns {'ok', n,
// slot, type, step, ..., inBandCount}.
var scriptAppend = newRedisScript(luaPrelude + `
local now, mode, force = ARGV[3], ARGV[4], ARGV[6] == '1'
local nowN = tonumber(now)
local contexts, hooks, hooksDirty, hooksNew = {}, {}, {}, {}
local tokens, tokensDirty, tokenTTL = {}, {}, {}

local function hook(id)
  if hooks[id] == nil then
    local raw = redis.call('GET', k('hook', id))
    if raw then hooks[id] = cjson.decode(raw) else hooks[id] = false end
  end
  return hooks[id] or nil
end
local function owner(token)
  if token == nil or token == '' then return nil end
  if tokens[token] == nil then tokens[token] = redis.call('GET', k('token', token)) or false end
  return tokens[token] or nil
end
local function setOwner(token, id)
  tokens[token] = id or false
  tokensDirty[token] = true
end
local function release(token, id)
  if owner(token) == id then setOwner(token, nil) end
end
local function expire(token)
  local id = owner(token)
  if not id then return end
  local h = hook(id)
  if not h then setOwner(token, nil) return end
  if terminal(redis.call('HGET', k('run', h.runId), 'status')) and (not h.tokenRetentionUntil or tonumber(h.tokenRetentionUntil) <= nowN) then
    setOwner(token, nil)
    h.tokenRetentionUntil = nil
    hooksDirty[id] = true
  end
end

local function load(id)
  if contexts[id] then return contexts[id] end
  local flat = redis.call('HGETALL', k('run', id))
  if #flat == 0 then return nil end
  local c = {id = id, run = {}, set = {}, del = {}, attrs = {}, steps = {}, stepsDirty = {}, stepsNew = {}, last = {}, lastDirty = {}, records = {}, corr = {}}
  for i = 1, #flat, 2 do c.run[flat[i]] = flat[i + 1] end
  c.status0 = c.run.status
  c.count = tonumber(c.run.count)
  contexts[id] = c
  return c
end
local function step(c, id)
  if c.steps[id] == nil then
    local raw = redis.call('HGET', k('steps', c.id), id)
    if raw then c.steps[id] = cjson.decode(raw) else c.steps[id] = false end
  end
  return c.steps[id] or nil
end
local function lastField(t, corr)
  if t == 'run_started' or t == 'run_cancelled' then return t end
  return t .. '\n' .. corr
end
local function last(c, field)
  if c.last[field] == nil then c.last[field] = redis.call('HGET', k('last', c.id), field) or false end
  return c.last[field] or nil
end
local function setRun(c, field, value)
  c.run[field] = value
  if value == nil then c.set[field] = nil c.del[field] = true else c.set[field] = value c.del[field] = nil end
end

local function admit(c, d)
  local t, corr = d.t, d.c
  local status = c.run.status
  if terminal(status) and not ((t == 'step_completed' or t == 'step_failed') and step(c, corr) and step(c, corr).status == 'running') then
    if t == 'run_cancelled' and status == 'cancelled' then return {replay = last(c, t) or 0} end
    return {err = 'run_terminal'}
  end
  if t == 'run_started' and status == 'running' then return {replay = last(c, t) or 0} end
  if t == 'step_created' and step(c, corr) then
    return {replay = last(c, lastField(t, corr)) or step(c, corr).slot or 0}
  end
  if t == 'step_started' then
    local s = step(c, corr)
    if not s or (s.status ~= 'pending' and s.status ~= 'running') then return {err = 'step_not_startable'} end
  end
  if t == 'step_completed' or t == 'step_failed' or t == 'step_retrying' then
    local s = step(c, corr)
    if not s or s.status ~= 'running' then return {err = 'step_not_startable'} end
  end
  if t == 'hook_disposed' then
    local h = hook(corr)
    if not h or h.runId ~= c.id then return {err = 'hook_not_active'} end
    if h.disposedAt then
      local slot = last(c, lastField(t, corr))
      if slot then return {replay = slot} end
    end
  end
  if t == 'wait_completed' then
    local slot = last(c, lastField(t, corr))
    if slot then return {replay = slot} end
  end
  if t == 'hook_received' then
    local h = hook(corr)
    if not h or h.runId ~= c.id or h.disposedAt or owner(h.token) ~= corr then
      if h and h.forceClaimedBy then return {err = 'hook_force_claimed'} end
      return {err = 'hook_not_active'}
    end
    if h.webhook and type(d.recv) == 'table' then
      if d.recv.req and (not h.webhook or h.rid) then return {err = 'hook_not_active'} end
      if d.recv.resp and (not h.webhook or not h.manual or not h.rid or h.rid ~= d.recv.to) then return {err = 'hook_not_active'} end
    end
  end
  if t == 'hook_created' and mode ~= 'claim' and type(d.hook) == 'table' and (d.hook.token or '') ~= '' then
    expire(d.hook.token)
    if owner(d.hook.token) then return {err = 'hook_conflict'} end
  end
  return nil
end

local function complete(h, id)
  release(h.token, id)
  h.disposedAt = now
end

local stepStatus = {step_started = 'running', step_completed = 'completed', step_failed = 'failed', step_retrying = 'pending'}

local function materialize(c, d, slot)
  local t, corr = d.t, d.c
  setRun(c, 'updatedAt', now)
  if t == 'run_started' then
    setRun(c, 'status', 'running')
    setRun(c, 'startedAt', now)
  elseif t == 'run_completed' then
    setRun(c, 'status', 'completed')
    setRun(c, 'completedAt', now)
    setRun(c, 'resultSlot', fmt(slot))
  elseif t == 'run_failed' then
    setRun(c, 'status', 'failed')
    setRun(c, 'completedAt', now)
    setRun(c, 'resultSlot', fmt(slot))
  elseif t == 'run_cancelled' then
    setRun(c, 'status', 'cancelled')
    setRun(c, 'completedAt', now)
  elseif t == 'attr_set' then
    for _, change in ipairs(d.attrs or {}) do c.attrs[change.k] = change.v or false end
  elseif t == 'step_created' then
    local data = d.step or {}
    c.steps[corr] = {name = data.name or '', status = 'pending', attempt = 0, slot = slot, createdAt = now, updatedAt = now}
    c.stepsDirty[corr] = true
    c.stepsNew[corr] = slot
  elseif stepStatus[t] then
    local s = step(c, corr)
    if s then
      s.status, s.updatedAt = stepStatus[t], now
      if t == 'step_started' then
        s.attempt = (s.attempt or 0) + 1
        s.startedAt = s.startedAt or now
      elseif t == 'step_completed' or t == 'step_failed' then
        s.completedAt = now
      end
      if t ~= 'step_started' then s.resultSlot = slot end
      if type(d.res) == 'table' then s.retryAfter = d.res.retryAfter end
      c.stepsDirty[corr] = true
    end
  elseif t == 'hook_created' then
    local data = type(d.hook) == 'table' and d.hook or {}
    hooks[corr] = {runId = c.id, wf = c.run.wf, token = data.token or '', slot = slot, webhook = data.webhook, manual = data.manual, retentionUs = data.retentionUs, createdAt = now, updatedAt = now, claimedFrom = data.claimedFrom}
    if tonumber(data.retentionUs or 0) > 0 then hooks[corr].tokenRetentionUntil = fmt(nowN + tonumber(data.retentionUs)) end
    hooksDirty[corr] = true
    table.insert(hooksNew, {corr, c.id})
    if (data.token or '') ~= '' then setOwner(data.token, corr) end
  elseif t == 'hook_received' then
    local h = hook(corr)
    if h then
      h.updatedAt, h.receivedAt = now, now
      hooksDirty[corr] = true
      if h.webhook and type(d.recv) == 'table' then
        if d.recv.req then h.rid = slot end
        if d.recv.resp and h.rid and d.recv.to == h.rid then complete(h, corr) end
      end
      if h.webhook and not h.manual and h.rid then complete(h, corr) end
    end
  elseif t == 'hook_disposed' then
    local h = hook(corr)
    if h then release(h.token, corr) h.disposedAt, h.updatedAt, h.forceClaimedBy = now, now, d.taken hooksDirty[corr] = true end
  end
end

local function append(c, d)
  local slot = c.count + 1
  c.count = slot
  table.insert(c.records, d.r)
  if d.c ~= '' then table.insert(c.corr, {d.c, slot}) end
  local field = lastField(d.t, d.c)
  c.last[field] = slot
  c.lastDirty[field] = true
  materialize(c, d, slot)
  return slot
end

local function stepResult(c, d)
  if d.t ~= 'step_created' then return '' end
  local s = step(c, d.c)
  if not s then return '' end
  return cjson.encode(s)
end

local function delta(c)
  local start = tonumber(ARGV[5])
  if start < 0 then return c.count, {} end
  if start > c.count then start = c.count end
  if start == c.count then return start, {} end
  return start, redis.call('LRANGE', k('events', c.id), start, c.count - 1)
end

-- retainTokens releases the tokens of a run that became terminal, or expires
-- them when their hook's retention lapses, so token keys do not outlive it.
local function retainTokens(c)
  if not terminal(c.run.status) or terminal(c.status0) then return end
  local ids = redis.call('ZRANGE', k('runhooks', c.id), 0, -1)
  for _, entry in ipairs(hooksNew) do
    if entry[2] == c.id then table.insert(ids, entry[1]) end
  end
  for _, id in ipairs(ids) do
    local h = hook(id)
    if h and owner(h.token) == id then
      if h.tokenRetentionUntil and tonumber(h.tokenRetentionUntil) > nowN then
        tokenTTL[h.token] = id
      else
        setOwner(h.token, nil)
      end
    end
  end
end

local function flush()
  for _, c in pairs(contexts) do retainTokens(c) end
  for _, c in pairs(contexts) do
    if #c.records > 0 then
      rpush(k('events', c.id), c.records)
      for _, e in ipairs(c.corr) do redis.call('ZADD', k('corr', c.id .. ':' .. hex(e[1])), e[2], e[2]) end
      for field in pairs(c.lastDirty) do redis.call('HSET', k('last', c.id), field, c.last[field]) end
      setRun(c, 'count', fmt(c.count))
      for field, value in pairs(c.set) do redis.call('HSET', k('run', c.id), field, value) end
      for field in pairs(c.del) do redis.call('HDEL', k('run', c.id), field) end
      if c.run.status ~= c.status0 then
        local wf, score = c.run.wf, c.run.score
        redis.call('ZREM', P .. ':runs:status:' .. c.status0, c.id)
        redis.call('ZREM', P .. ':runs:wfstatus:' .. c.status0 .. ':' .. wf, c.id)
        redis.call('ZADD', P .. ':runs:status:' .. c.run.status, score, c.id)
        redis.call('ZADD', P .. ':runs:wfstatus:' .. c.run.status .. ':' .. wf, score, c.id)
      end
      for key, value in pairs(c.attrs) do
        if value then redis.call('HSET', k('attrs', c.id), key, value) else redis.call('HDEL', k('attrs', c.id), key) end
      end
      for id in pairs(c.stepsDirty) do redis.call('HSET', k('steps', c.id), id, cjson.encode(c.steps[id])) end
      for id, slot in pairs(c.stepsNew) do redis.call('ZADD', k('steporder', c.id), slot, id) end
    end
  end
  for id in pairs(hooksDirty) do redis.call('SET', k('hook', id), cjson.encode(hooks[id])) end
  for _, entry in ipairs(hooksNew) do
    local score = fmt(nextScore(nowN))
    redis.call('ZADD', P .. ':hooks', score, entry[1])
    redis.call('ZADD', k('runhooks', entry[2]), score, entry[1])
  end
  for token in pairs(tokensDirty) do
    if tokens[token] then redis.call('SET', k('token', token), tokens[token]) else redis.call('DEL', k('token', token)) end
  end
  for token, id in pairs(tokenTTL) do
    local h = hooks[id]
    if tokens[token] == id and h and h.tokenRetentionUntil then
      redis.call('PEXPIRE', k('token', token), math.ceil((tonumber(h.tokenRetentionUntil) - nowN) / 1000))
    end
  end
end

local c = load(ARGV[2])
if not c then return {'err', 'run_not_found'} end
local fence = ARGV[7]
local inBand = tonumber(c.run.inband or '0')
if fence ~= '' and tonumber(fence) ~= inBand then return {'err', 'in_band_superseded'} end
local function advance(n)
  if fence == '' then return end
  inBand = inBand + n
  setRun(c, 'inband', fmt(inBand))
end

if mode == 'batch' then
  local out = {'ok', 0}
  for i = 8, #ARGV do
    local d = cjson.decode(ARGV[i])
    if d.bad then return {'err', 'invalid'} end
    local admitted = admit(c, d)
    if admitted then
      if admitted.err then return {'err', admitted.err} end
      return {'err', 'invalid'}
    end
    local slot = append(c, d)
    table.insert(out, slot)
    table.insert(out, d.t)
    table.insert(out, stepResult(c, d))
    out[2] = out[2] + 1
  end
  advance(out[2])
  flush()
  table.insert(out, inBand)
  return out
end

local d = cjson.decode(ARGV[8])
local forced = ''
local start, records
if mode == 'claim' then
  if terminal(c.run.status) then return {'err', 'run_terminal'} end
  start, records = delta(c)
  local token = d.hook.token
  expire(token)
  local id = owner(token)
  if id then
    local h = hook(id)
    if not force then
      d.t = 'hook_conflict'
      d.payload.conflictingRunId = h.runId
    else
      d.payload.forceClaimedFrom = {runId = h.runId, hookId = id}
      d.hook.claimedFrom = d.payload.forceClaimedFrom
      local oc = load(h.runId)
      if not oc or terminal(oc.run.status) then setOwner(token, nil) else
        append(oc, {t = 'hook_disposed', c = id, taken = {runId = c.id, hookId = d.c}, r = cjson.encode({t = 'hook_disposed', c = id, d = ARGV[9], at = now})})
        forced = oc.id
      end
    end
  end
  local record = cjson.decode(d.r)
  record.t, record.j, record.d = d.t, d.payload, nil
  d.r = cjson.encode(record)

else
  start, records = delta(c)
  local admitted = admit(c, d)
  if admitted then
    if admitted.err then return {'err', admitted.err} end
    local out = {'ok', tonumber(admitted.replay), d.t, 0, stepResult(c, d), '', start, inBand}
    for _, record in ipairs(records) do table.insert(out, record) end
    return out
  end
end
local allocated = #c.records + 1
for _, record in ipairs(c.records) do table.insert(records, record) end
local slot = append(c, d)
advance(allocated)
flush()
local out = {'ok', slot, d.t, allocated, stepResult(c, d), forced, start, inBand}
for _, record in ipairs(records) do table.insert(out, record) end
return out
`)

// scriptLoadRuns takes ARGV prefix, resolve flag, runIDs... It returns per run
// {hash, attrs, input, result}: flat field/value arrays, then the run_created
// record when the flag is '1' and the record at resultSlot when the flag is
// '1' or the run failed, each empty when not read. A missing run has an empty
// hash.
var scriptLoadRuns = newRedisScript(luaPrelude + `
local all = ARGV[2] == '1'
local out = {}
for i = 3, #ARGV do
  local id = ARGV[i]
  local hash = redis.call('HGETALL', k('run', id))
  local input, result = '', ''
  if #hash > 0 then
    local fields = {}
    for j = 1, #hash, 2 do fields[hash[j]] = hash[j + 1] end
    if all then input = redis.call('LINDEX', k('events', id), 0) or '' end
    if fields.resultSlot and (all or fields.status == 'failed') then
      result = redis.call('LINDEX', k('events', id), tonumber(fields.resultSlot) - 1) or ''
    end
  end
  table.insert(out, {hash, redis.call('HGETALL', k('attrs', id)), input, result})
end
return out
`)

// scriptEventsAt takes ARGV prefix, then runID and slot pairs... It returns
// the records, empty for a missing one.
var scriptEventsAt = newRedisScript(luaPrelude + `
local out = {}
for i = 2, #ARGV, 2 do table.insert(out, redis.call('LINDEX', k('events', ARGV[i]), tonumber(ARGV[i + 1]) - 1) or '') end
return out
`)

const luaStream = `
local run, name = ARGV[2], ARGV[3]
local meta = k('stream', run .. ':' .. hex(name))
local chunks = k('chunks', run .. ':' .. hex(name))
redis.call('SADD', k('streams', run), name)
local closed = redis.call('HGET', meta, 'closed') == '1'
local session = redis.call('HGET', meta, 'session') or ''
`

// scriptStreamCreateSession takes ARGV prefix, runID, name, sessionID.
var scriptStreamCreateSession = newRedisScript(luaPrelude + luaStream + `
if closed then return {'err', 'run_terminal'} end
if session ~= '' then return {'err', 'stream_locked'} end
redis.call('HSET', meta, 'session', ARGV[4], 'seq', '0')
return {'ok'}
`)

// scriptStreamWriteSession takes ARGV prefix, runID, name, sessionID,
// sequence, chunks...
var scriptStreamWriteSession = newRedisScript(luaPrelude + luaStream + `
if closed or ARGV[4] == '' or session ~= ARGV[4] then return {'err', 'stream_locked'} end
if tonumber(redis.call('HGET', meta, 'seq') or '0') ~= tonumber(ARGV[5]) then return {'err', 'stream_locked'} end
local values = {}
for i = 6, #ARGV do table.insert(values, ARGV[i]) end
rpush(chunks, values)
redis.call('HINCRBY', meta, 'seq', 1)
return {'ok'}
`)

// scriptStreamCloseSession takes ARGV prefix, runID, name, sessionID, close
// flag.
var scriptStreamCloseSession = newRedisScript(luaPrelude + luaStream + `
if ARGV[4] == '' or session ~= ARGV[4] then return {'err', 'stream_locked'} end
redis.call('HDEL', meta, 'session')
redis.call('HSET', meta, 'seq', '0')
if ARGV[5] == '1' then redis.call('HSET', meta, 'closed', '1') end
return {'ok'}
`)

// scriptStreamWrite takes ARGV prefix, runID, name, chunks... for direct
// writes.
var scriptStreamWrite = newRedisScript(luaPrelude + luaStream + `
if closed or session ~= '' then return {'err', 'stream_locked'} end
local values = {}
for i = 4, #ARGV do table.insert(values, ARGV[i]) end
rpush(chunks, values)
return {'ok'}
`)

// scriptStreamClose takes ARGV prefix, runID, name.
var scriptStreamClose = newRedisScript(luaPrelude + luaStream + `
if session ~= '' then return {'err', 'stream_locked'} end
redis.call('HSET', meta, 'closed', '1')
return {'ok'}
`)

// scriptStreamRead takes ARGV prefix, runID, name, start, count. It returns
// {closed, length, chunks...} without creating the stream.
var scriptStreamRead = newRedisScript(luaPrelude + `
local meta = k('stream', ARGV[2] .. ':' .. hex(ARGV[3]))
local chunks = k('chunks', ARGV[2] .. ':' .. hex(ARGV[3]))
local closed = redis.call('HGET', meta, 'closed') == '1' and '1' or '0'
local length = redis.call('LLEN', chunks)
local out = {closed, length}
local start, count = tonumber(ARGV[4]), tonumber(ARGV[5])
if count > 0 and start < length then
  for _, chunk in ipairs(redis.call('LRANGE', chunks, start, start + count - 1)) do table.insert(out, chunk) end
end
return out
`)

// scriptQueuePut takes ARGV prefix, messageID, record, NX|XX. It returns 1
// when set.
var scriptQueuePut = newRedisScript(luaPrelude + `
if redis.call('SET', k('queue', ARGV[2]), ARGV[3], ARGV[4]) then return 1 end
return 0
`)

// scriptQueueTake takes ARGV prefix, messageID. It deletes and returns the
// record.
var scriptQueueTake = newRedisScript(luaPrelude + `
local key = k('queue', ARGV[2])
local record = redis.call('GET', key)
if not record then return false end
redis.call('DEL', key)
return record
`)

var allScripts = map[string]*redisScript{
	"createRun":          scriptCreateRun,
	"append":             scriptAppend,
	"loadRuns":           scriptLoadRuns,
	"eventsAt":           scriptEventsAt,
	"streamCreate":       scriptStreamCreateSession,
	"streamWriteSession": scriptStreamWriteSession,
	"streamCloseSession": scriptStreamCloseSession,
	"streamWrite":        scriptStreamWrite,
	"streamClose":        scriptStreamClose,
	"streamRead":         scriptStreamRead,
	"queuePut":           scriptQueuePut,
	"queueTake":          scriptQueueTake,
}
