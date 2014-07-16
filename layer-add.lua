
local entries   = ARGV[1]
local precision = ARGV[2]
local hash      = redis.sha1hex(ARGV[3])

-- This uses a variation on:
-- 'Less Hashing, Same Performance: Building a Better Bloom Filter'
-- http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf
local h = { }
h[0] = tonumber(string.sub(hash, 1 , 8 ), 16)
h[1] = tonumber(string.sub(hash, 9 , 16), 16)
h[2] = tonumber(string.sub(hash, 17, 24), 16)
h[3] = tonumber(string.sub(hash, 25, 32), 16)

for layer=1,32 do
  local key    = KEYS[1] .. ':' .. layer .. ':'
  local count_key = key .. 'count'
  local count = redis.call('INCR', count_key)
  local factor = math.ceil((entries + count) / entries)
  -- 0.69314718055995 = ln(2)
  local index  = math.ceil(math.log(factor) / 0.69314718055995)
  local scale  = math.pow(2, index - 1) * entries

  key = key .. index

  -- Based on the math from: http://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives
  -- Combined with: http://www.sciencedirect.com/science/article/pii/S0020019006003127
  -- 0.4804530139182 = ln(2)^2
  local bits = math.floor(-(scale * math.log(precision * math.pow(0.5, index))) / 0.4804530139182)

  -- 0.69314718055995 = ln(2)
  local k = math.floor(0.69314718055995 * bits / scale)

  local found = true
  for i=1, k do
    if redis.call('SETBIT', key, (h[i % 2] + i * h[2 + (((i + (i % 2)) % 4) / 2)]) % bits, 1) == 0 then
      found = false
    end
  end

  -- set expiration on new keys
  if count == 1 then
    if layer == 1 then
      redis.call('EXPIRE', count_key, ARGV[4])
    else
      local expire = redis.call('PTTL', KEYS[1] .. ':1:count')
      redis.call('PEXPIRE', count_key, expire)
    end
  end

  -- if it wasn't found in this layer already: break, after setting expiration
  if found == false then
    if count == 1 or (index > 1 and index - 1 == math.ceil(math.log(math.ceil((entries + count - 1) / entries)) / 0.69314718055995)) then
      -- always grab the expire from the layer 1 count key
      local expire = redis.call('PTTL', KEYS[1] .. ':1:count')
      redis.call('PEXPIRE', key, math.max(0, expire))
    end
    break
  end

end

