
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
  local key   = KEYS[1] .. ':' .. layer .. ':'
  local keyc  = key .. 'count'
  local count = redis.call('GET', keyc)

  if not count then
    count = 0
  end
  count = count + 1

  local factor = math.ceil((entries + count) / entries)
  -- 0.69314718055995 = ln(2)
  local index  = math.ceil(math.log(factor) / 0.69314718055995)
  local scale  = math.pow(2, index - 1) * entries


  -- Based on the math from: http://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives
  -- Combined with: http://www.sciencedirect.com/science/article/pii/S0020019006003127
  -- 0.69314718055995 = ln(2)
  -- 0.4804530139182 = ln(2)^2
  local maxk = math.floor(0.69314718055995 * math.floor((scale * math.log(precision * math.pow(0.5, index))) / -0.4804530139182) / scale)
  local b    = { }
  local k
  local keyn
  local bits
  local inlayer = false

  for i=1, maxk do
    table.insert(b, h[i % 2] + i * h[2 + (((i + (i % 2)) % 4) / 2)])
  end
    
  for n=1, index do
    keyn        = key .. n
    local found = true
    local scale = math.pow(2, n - 1) * entries

    -- 0.4804530139182 = ln(2)^2
    bits = math.floor((scale * math.log(precision * math.pow(0.5, n))) / -0.4804530139182)

    -- 0.69314718055995 = ln(2)
    k = math.floor(0.69314718055995 * bits / scale)

    for i=1, k do
      if redis.call('GETBIT', keyn, b[i] % bits) == 0 then
        found = false
        break
      end
    end

    if found then
      inlayer = true
      break
    end
  end

  if inlayer == false then

    -- was not found, now add to this layer in the last filter in the scaling sequence
    for i=1, k do
      redis.call('SETBIT', keyn, b[i] % bits, 1)
    end

    count = redis.call('INCR', keyc)

    -- set expiration on new keys
    if count == 1 then
      if layer == 1 then
        redis.call('EXPIRE', keyc, ARGV[4])
      else
        -- always grab the expire from the layer 1 count key
        local expire = redis.call('PTTL', KEYS[1] .. ':1:count')
        redis.call('PEXPIRE', keyc, expire)
      end
    end
    if count == 1 or (index > 1 and index - 1 == math.ceil(math.log(math.ceil((entries + count - 1) / entries)) / 0.69314718055995)) then
      -- always grab the expire from the layer 1 count key
      local expire = redis.call('PTTL', KEYS[1] .. ':1:count')
      redis.call('PEXPIRE', keyn, math.max(0, expire))
    end

    return layer - 1
  end
end

-- You're already in the top layer... sorry
return 32

