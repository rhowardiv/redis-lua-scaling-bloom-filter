var fs = require('fs'),
  redis = require('redis'),
  stdio = require('stdio'),

  options = stdio.getopt({
    'size': { key: 's', args: 1, description: 'Initial bloom filter size (10000)' },
    'error': { key: 'e', args: 1, description: 'Target false positive rate (0.01)' },
    'count': { key: 'c', args: 1, description: 'Number of elements to add (100000)' },
    'host': { key: 'h', args: 1, description: 'Redis host (127.0.0.1)' },
    'port': { key: 'p', args: 1, description: 'Redis port (6379)' },
  }),

  size = options.size || 10000,
  error = options.error || 0.01,
  count = options.count || 100000,
  client = redis.createClient(
    options.port || 6379,
    options.host || '127.0.0.1'
  ),

  found = 0,
  start;

console.log('size  = ' + size);
console.log('error = ' + error);
console.log('count = ' + count);


function check(n, checksha) {
  if (n == count) {
    var sec = count / ((Date.now() - start) / 1000);
    console.log(sec + ' per second');

    console.log((found / (count / 100)) + '% false positives');

    console.log('done.');
    process.exit();
    return;
  }

  var id = Math.round(Math.random() * 4000000000);

  client.evalsha(checksha, 0, 'test', size, error, id, function(err, yes) {
    if (err) {
      throw err;
    }

    if (yes) {
      ++found;
    }

    check(n + 1, checksha);
  });
}


client.send_command(
  'script',
  ['load', fs.readFileSync('check.lua', 'ascii')],
  function(err, sha) {
    var checksha = '';
    if (err) {
      throw err;
    }

    checksha = sha;
    
    console.log('checking...');

    start = Date.now();

    check(0, checksha);
  }
);
