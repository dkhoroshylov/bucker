var dgram, redis;

function loadRedis() {
    try {
        redis = require('redis');
    } catch (e) {
        throw new Error('Attempted to use redis transport without installing the redis module');
    }
}

var Logstash = module.exports = function (opts, name) {
    if (!(this instanceof Logstash)) return new Logstash(opts, name);

    this.name = name || '';
    this.host = opts.host || '127.0.0.1';
    this.source = opts.source;
    this.source_host = opts.source_host;
    this.version = opts.version || 1;
    this.defaultPacket = opts.defaultPacket || {};
    this.key = opts.key || 'bucker';

    if (opts.redis) {
        loadRedis();
        this.redis = true;
        this.port = opts.port || 6379;
        this.channel = opts.hasOwnProperty('channel') ? opts.channel : true;
        this.list = !this.channel;
        this.client = redis.createClient(this.port, this.host, {retry_max_delay: 5000});
        this.client.on('error', function (err) {
          //swallow error and let it keep retrying
        });
    } else if (opts.udp) {
        dgram = require('dgram');
        this.udp = true;
        this.port = opts.port || 9999;
        this.client = dgram.createSocket('udp4');
    }
    if (opts.file) {
        var mkdirp = require('mkdirp'),
            path = require('path'),
            fs = require('fs');

        this.filename = opts.file || 'logstash.log';
        mkdirp.sync(path.dirname(this.filename));
        this.filestream = fs.createWriteStream(this.filename, { encoding: 'utf8', flags: 'a+' });
    }
};

Logstash.prototype.getDefaultPacket = function(time){
  var packet = {};

  packet['@version'] = this.version;
  packet['@timestamp'] = time.toISOString();

  var keys = Object.keys(this.defaultPacket);
  var i = keys.length;
  while (i--) {
    packet[keys[i]] = this.defaultPacket[keys[i]];
  }
  return packet;
}

Logstash.prototype.log = function (time, level, module, data, tags) {
    var packet = this.getDefaultPacket(time);
    var name = module || this.name;
    var source = this.source || name;

    packet['tags'] = tags;
    packet['type'] = this.key;
    packet['source'] = source;
    if (this.source_host) packet['source_host'] = this.source_host;
    packet['module'] = name;
    packet['level'] = level.toUpperCase();
    packet['message'] = data;

    this.send(packet);
};

Logstash.prototype.access = function (module, data, tags) {
    var packet = this.getDefaultPacket(data.time);
    var name = module || this.name;
    var source = this.source || name;

    packet['tags'] = tags;
    packet['type'] = this.key + '_access';
    packet['source'] = source;
    if (this.source_host) packet['source_host'] = this.source_host;
    packet['url'] = data.url;
    packet['client'] = data.remote_ip;
    packet['size'] = data.length;
    packet['responsetime'] =  data.response_time;
    packet['status'] = data.status;
    packet['method'] = data.method;
    packet['http_referrer'] = data.referer;
    packet['http_user_agent'] = data.agent;
    packet['http_version'] = data.http_ver;

    packet['message'] = [data.method, data.url, data.status].join(' ');

    this.send(packet);
};

Logstash.prototype.exception = function (time, module, err, tags) {
    var packet = this.getDefaultPacket(time);
    var name = module || this.name;
    var source = this.source || name;

    packet['tags'] = tags;
    packet['type'] = this.key;
    packet['source'] = source;
    if (this.source_host) packet['source_host'] = this.source_host;
    packet['module'] = name;
    packet['level'] = 'EXCEPTION';
    packet['stack'] = err.stack.split('\n');
    packet['message'] = err.stack;

    this.send(packet);
};

Logstash.prototype.stat = function (time, module, statName, type, value, tags) {
    var packet = this.getDefaultPacket(time);
    var name = module || this.name;
    var source = this.source || name;

    packet['tags'] = tags;
    packet['type'] = this.key;
    packet['source'] = source;
    if (this.source_host) packet['source_host'] = this.source_host;
    packet['module'] = name;
    packet['level'] = 'STAT';
    packet['name'] = statName
    packet['type'] = type;
    packet['value'] = value;
    packet['message'] = statName + '(' + type + '): ' + value;

    this.send(packet);
};

Logstash.prototype.send = function (data) {
    var packet = JSON.stringify(data);

    if (this.redis) {
        if (this.channel) {
            this.client.publish(this.key, packet);
        } else {
            this.client.rpush(this.key, packet);
        }
    } else if (this.udp) {
        packet = new Buffer(packet);
        this.client.send(packet, 0, packet.length, this.port, this.host);
    }
    if (this.filename) {
        this.filestream.write(packet + '\n');
    }
};