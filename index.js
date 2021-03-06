"use strict";
var util = require('util'),
    fs = require('fs'),
    helenus = require('helenus'),
    Q = require('q');

function CassandraBackend(config) {
    var self = this;
    var pool;
    var connect = Q.defer();
    var cache = Q.defer();

    this.connect = function () {
        if (!pool) {
            pool = true;
            var options = {
                hosts: config.backendconf.cassandra.hosts || ['localhost:9160'],
                keyspace: 'system'
            };
            if (config.backendconf.cassandra.user && config.backendconf.cassandra.password) {
                options.user = config.backendconf.cassandra.user;
                options.password = config.backendconf.cassandra.password;
            }
            pool = new helenus.ConnectionPool(options);
            pool.connect(function(err, ks) {
                pool.createKeyspace(self.namespace, config.backendconf.cassandra.keyspaceconf || { }, function (err) {
                    pool.cql('USE ' + self.namespace, function (err, res) {
                        if (err) return connect.reject(err);
                        connect.resolve(pool);
                        self.connected = true;
                    });
                });
            });
        }
        return connect.promise;
    }

    this.wait = function (callback) {
        if (callback) {
            self.connect().done(callback);
        } else {
            return self.connect();
        }
    };

    this.get = function (col, keys, callback) {
        self.connect().done(function () {
            pool.cql('CREATE COLUMNFAMILY ' + col + '(key TEXT PRIMARY KEY, value TEXT, metadata TEXT)', function (err, res) {
                var query = 'SELECT key, value FROM ' + col;
                var params = [ ];
                if (keys !== '*') {
                    query += ' WHERE KEY ';
                    if (util.isArray(keys)) {
                        query += 'IN (';
                        for (var ii; ii < keys.length; ii++) {
                            query += (ii ? ', ' : '') + '?';
                        }
                        query += ')';
                        params = keys;
                    } else {
                        query += '= ?';
                        params = [ keys];
                    }
                }
                pool.cql(query, params, function (err, recs) {
                    if (recs && (keys === '*' || util.isArray(keys))) {
                        var results = { };
                        recs.forEach(function (row) {
                            results[row[0].value] = JSON.parse(row[1].value);
                        });
                        callback(err, results);
                    } else if (recs && recs[0] && recs[0][1]) {
                        callback(err, JSON.parse(recs[0][1].value));
                    } else {
                        callback(err, null);
                    }
                });
            });
        }, function (err) { callback(err, null); });
    }; 

    this.set = function (col, key, object, callback, options) {
        options = options || { };
        options.metadata = options.metadata || { };
        self.connect().done(function () {
            pool.cql('CREATE COLUMNFAMILY ' + col + '(key TEXT PRIMARY KEY, value TEXT, metadata TEXT)', function (err, res) {
                var query = 'INSERT INTO ' + col + ' (KEY, value, metadata) VALUES (?, ?, ?)';
                if (options.expiration) {
                    query += ' USING TTL=' + options.expiration;
                }
                pool.cql(query, [key, JSON.stringify(object), JSON.stringify(options.metadata)], function (err, res) {
                    if (typeof callback === 'function') callback(err, res);
                });
            });
        }, function(err) { callback(err, null); });
    };

    this.del = function (col, key, callback) {
        self.connect().done(function () {
            pool.cql('DELETE FROM ' + col + ' WHERE KEY = ?', [key], function (err, result) {
                if (typeof callback === 'function') callback(err, result);
            });
        }, function(err) { callback(err, null); });
    };

    self.media = {
        send: function (recordid, name, res) {
            self.connect().done(function () {
                pool.cql('SELECT value, metadata FROM media WHERE KEY = ?', [ recordid + '_' + name ], function (err, file) {
                    if (file && file.length > 0) {
                        var metadata = JSON.parse(file[0][1].value);
                        res.setHeader('Content-type', metadata.content_type);
                        res.send(new Buffer(JSON.parse(file[0][0].value)));
                    } else {
                        res.send(404);
                    }
                });
            });
        },
        save: function (recordid, name, metadata, tmppath, callback) {
            self.connect().done(function () {
                fs.readFile(tmppath, function (err, data) {
                    self.set('media', recordid + '_' + name, data, function () {
                        fs.unlink(tmppath, function () {
                            callback(err);
                        });
                    }, { metadata: metadata });
                });
            });
        },
        del: function (recordid, name, callback) {
            self.del('media', recordid + '_' + name, callback);
        }
    };

    config.backendconf = config.backendconf || { };
    config.backendconf.cassandra = config.backendconf.cassandra || { };
    self.namespace = config.backendconf.cassandra.namespace || 'biblionarrator';
    //self.cacheexpire = config.cacheconf.defaultexpiry || 600;
    self.connected = false;

}

module.exports = CassandraBackend;
module.exports.description = 'Cassandra backend (clusterable)';
module.exports.features = {
    datastore: true,
    mediastore: true/*,
    cache: true*/
};
