var fs = require('fs'),
  redis = require('redis'),
  stdio = require('stdio'),

  options = stdio.getopt({
    'size': { key: 's', args: 1, description: 'Initial bloom filter size (10000)' },
    'error': { key: 'e', args: 1, description: 'Target false positive rate (0.01)' },
    'layers': { key: 'l', args: 1, description: 'Number of layers to test' },
    'count': { key: 'c', args: 1, description: 'Number of elements to add (100000)' },
    'host': { key: 'h', args: 1, description: 'Redis host (127.0.0.1)' },
    'port': { key: 'p', args: 1, description: 'Redis port (6379)' },
  }),

  size = options.size || 10000,
  error = options.error || 0.01,
  layers = options.layers || 10,
  count = options.count || 100000,
  layersize = count / layers,
  client = redis.createClient(
    options.port || 6379,
    options.host || '127.0.0.1'
  ),

  added = [],
  wrong = 0,
  start;




console.log('size   = ' + size);
console.log('error  = ' + error);
console.log('count  = ' + count);
console.log('layers = ' + layers);


function check(n, checksha) {
  if (n == count) {
    var sec = count / ((Date.now() - start) / 1000);
    console.log(sec + ' per second');
      
    console.log((wrong / (count / 100)) + '% in wrong layer');

    console.log('done.');
    process.exit();
    return;
  }

  client.evalsha(checksha, 0, 'test', size, error, added[n], function(err, found) {
    if (err) {
      throw err;
    }

    var layer = 1 + Math.floor(n / layersize);

    if (found != layer) {
      //console.log(added[n] + ' expected in ' + layer + ' found in ' + found + '!');
      ++wrong;
    }

    check(n + 1, checksha);
  });
}


function add(n, layer, id, addsha, checksha) {
  if (n == count) {
    var sec = (count * (layers / 2)) / ((Date.now() - start) / 1000);
    console.log(sec + ' per second');

    console.log('checking...');
    
    start = Date.now();

    check(0, checksha);
    return;
  }

  if (!id) {
    id = Math.round(Math.random() * 4000000000);

    added.push(id);
  }

  if (!layer) {
    layer = 1 + Math.floor(n / layersize);
  }

  client.evalsha(addsha, 0, 'test', size, error, id, function(err) {
    if (err) {
      throw err;
    }

    if (layer == 1) {
      add(n + 1, null, null, addsha, checksha);
    } else {
      add(n, layer - 1, id, addsha, checksha)
    }
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

      add(0, null, null, addsha, checksha);
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
        fs.readFileSync('layer-add.lua', 'ascii'),
        fs.readFileSync('layer-check.lua', 'ascii')
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
