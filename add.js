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

  added = [],
  start;

console.log('size  = ' + size);
console.log('error = ' + error);
console.log('count = ' + count);


function check(n, checksha) {
  if (n == count) {
    var sec = count / ((Date.now() - start) / 1000);
    console.log(sec + ' per second');

    console.log('done.');
    process.exit();
    return;
  }

  client.evalsha(checksha, 0, 'test', size, error, added[n], function(err, found) {
    if (err) {
      throw err;
    }

    if (!found) {
      console.log(added[n] + ' was not found!');
    }

    check(n + 1, checksha);
  });
}


function add(n, addsha, checksha) {
  if (n == count) {
    var sec = count / ((Date.now() - start) / 1000);
    console.log(sec + ' per second');

    console.log('checking...');
    
    start = Date.now();

    check(0, checksha);
    return;
  }

  var id = Math.round(Math.random() * 4000000000);

  added.push(id);

  client.evalsha(addsha, 0, 'test', size, error, id, function(err) {
    if (err) {
      throw err;
    }

    add(n + 1, addsha, checksha);
  });
}


function load(addsource, checksource) {
  var addsha = '',
    checksha = '';
  client.send_command('script', ['load', addsource], function(err, sha) {
    if (err) {
      throw err;
    }

    addsha = sha;

    client.send_command('script', ['load', checksource], function(err, sha) {
      if (err) {
        throw err;
      }

      checksha = sha;

      console.log('adding...');

      start = Date.now();

      add(0, addsha, checksha);
    });
  });
}


client.keys('test:*', function(err, keys) {
  if (err) {
    throw err;
  }

  console.log('clearing...');

  (function clear(i) {
    if (i == keys.length) {
      load(
        fs.readFileSync('add.lua', 'ascii'),
        fs.readFileSync('check.lua', 'ascii')
      );
      return;
    }

    client.del(keys[i], function(err) {
      if (err) {
        throw err;
      }

      clear(i + 1);
    });
  }(0));
});
