'use strict';

var zookeeper = require('node-zookeeper-client'),
    util = require('util'),
    async = require("async"),
    EventEmiter = require('events').EventEmitter,
    Exception = zookeeper.Exception,
    debug = require('debug')('kafka-node:zookeeper');

/**
 * Provides kafka specific helpers for talking with zookeeper
 *
 * @param {String} [connectionString='localhost:2181/kafka0.8'] A list of host:port for each zookeeper node and
 *      optionally a chroot path
 *
 * @constructor
 */
var Zookeeper = function (connectionString, options) {
    this.client = zookeeper.createClient(connectionString, options);

    var that = this;
    this.client.on('connected', function () {
        that.listBrokers();
    });
    this.client.connect();
};

var consumersInList = {};

util.inherits(Zookeeper, EventEmiter);

function createMetadata (payloads) {
    var metadata = '{"version":1,"subscription":';
    metadata += '{';
    var sep = '';
    payloads.map(function (p) {
        metadata += sep + '"' + p.topic + '": 1';
        sep = ', ';
    });
    metadata += '}';
    var milliseconds = Date.now();
    metadata += ',"pattern":"white_list","timestamp":"' + milliseconds + '"}';

    return metadata;
}

Zookeeper.prototype.registerConsumer = function (groupId, consumerId, payloads, cb) {
    var path = '/consumers/' + groupId + '/ids';
    var that = this;

    async.series([
          function (callback) {
              that.client.mkdirp(
                path,
                function (error, path) {
                    // simply carry on
                    callback();
                });
          },
          function (callback) {
              var metadata = createMetadata(payloads);
              that.client.create(path + '/' +consumerId, new Buffer(metadata), zookeeper.CreateMode.EPHEMERAL, function (error, stat) {
                  if (error) {
                      callback(error);
                  }
                  else {
                      debug('Node: %s was created.', path + '/' + consumerId);
                      cb();
                  }
              });
          }],
      function (err) {
          if (err) cb(err);
          else cb();
      });
};

Zookeeper.prototype.updateConsumer = function (groupId, consumerId, payloads, cb) {
    var path = '/consumers/' + groupId + '/ids/' + consumerId;

    var metadata = createMetadata(payloads);
    this.client.setData(path, new Buffer(metadata), function (error, stat) {
        if (error) {
            callback(error);
        }
        else {
            debug('Node: %s was updated.', path);
            cb();
        }
    });
};

Zookeeper.prototype.getConsumersPerTopic = function (groupId, cb) {
    var consumersPath = '/consumers/' + groupId + '/ids';
    var brokerTopicsPath = '/brokers/topics';
    var consumers = [];
    var path = '/consumers/' + groupId;
    var that = this;
    var consumerPerTopicMap = new ZookeeperConsumerMappings();

    async.series([
            function (callback) {
                that.client.getChildren(consumersPath, function (error, children, stats) {
                    if (error) {
                        callback(error);
                        return;
                    }
                    else {
                        debug('Children are: %j.', children);
                        consumers = children;
                        async.each(children, function (consumer, cbb) {
                            var path = consumersPath + '/' + consumer;
                            that.client.getData(
                                path,
                                function (error, data) {
                                    if (error) {
                                        cbb(error);
                                    }
                                    else {
                                        try {
                                            var obj = JSON.parse(data.toString());
                                            // For each topic
                                            for (var topic in obj.subscription) {
                                                if (consumerPerTopicMap.topicConsumerMap[topic] == undefined) {
                                                    consumerPerTopicMap.topicConsumerMap[topic] = [];
                                                }
                                                consumerPerTopicMap.topicConsumerMap[topic].push(consumer);

                                                if (consumerPerTopicMap.consumerTopicMap[consumer] == undefined) {
                                                    consumerPerTopicMap.consumerTopicMap[consumer] = [];
                                                }
                                                consumerPerTopicMap.consumerTopicMap[consumer].push(topic);
                                            }

                                            cbb();
                                        } catch (e) {
                                            debug(e);
                                            callback(new Error("Unable to assemble data"));
                                        }
                                    }
                                }
                            );
                        }, function (err) {
                            if (err)
                                callback(err);
                            else
                                callback();
                        });
                    }
                });
            },
            function (callback) {
                Object.keys(consumerPerTopicMap.topicConsumerMap).forEach(function (key) {
                    consumerPerTopicMap.topicConsumerMap[key] = consumerPerTopicMap.topicConsumerMap[key].sort();
                });
                callback();
            },
            function (callback) {
                async.each(Object.keys(consumerPerTopicMap.topicConsumerMap), function (topic, cbb) {
                    var path = brokerTopicsPath + '/' + topic;
                    that.client.getData(
                        path,
                        function (error, data) {
                            if (error) {
                                cbb(error);
                            }
                            else {
                                var obj = JSON.parse(data.toString());
                                // Get the topic partitions
                                var partitions = Object.keys(obj.partitions).map(function (partition) {
                                    return partition;
                                });
                                consumerPerTopicMap.topicPartitionMap[topic] = partitions.sort(compareNumbers);
                                cbb();
                            }
                        }
                    );
                }, function (err) {
                    if (err)
                        callback(err);
                    else
                        callback();
                });
            }],
        function (err) {
            if (err) {
                debug(err);
                cb(err);
            }
            else cb(null, consumerPerTopicMap);
        });
};

function compareNumbers(a, b) {
    return a - b;
}

Zookeeper.prototype.listBrokers = function (cb) {
    var that = this;
    var path = '/brokers/ids';
    this.client.getChildren(
        path,
        function () {
            that.listBrokers();
        },
        function (error, children) {
            if (error) {
                debug('Failed to list children of node: %s due to: %s.', path, error);
                that.emit('error', error);
                return;
            }

            if (children.length) {
                var brokers = {};
                async.each(children, getBrokerDetail, function (err) {
                    if (err) {
                        that.emit('error', error);
                        return;
                    }
                    if (!that.inited) {
                        that.emit('init', brokers);
                        that.inited = true;
                    } else {
                        that.emit('brokersChanged', brokers)
                    }
                    cb && cb(brokers); //For test
                });
            } else {
                if (that.inited)
                    return that.emit('brokersChanged', {})
                that.inited = true;
                that.emit('init', {});
            }

            function getBrokerDetail (id, cb) {
                var path = '/brokers/ids/' + id;
                that.client.getData(path,function (err, data) {
                    if (err) return cb(err);
                    brokers[id] = JSON.parse(data.toString());
                    cb();
                });
            }
        }
    );
};


Zookeeper.prototype.listConsumers = function (groupId) {
    var that = this;
    var path = '/consumers/' + groupId + '/ids';
    this.client.getChildren(
        path,
        function () {
            that.listConsumers(groupId);
        },
        function (error, children) {
            if (error && error.code === Exception.NO_NODE) {
                that.listConsumers(groupId);
            } else if (error) {
                that.emit('error', error);
            } else {
                that.dataOfConsumers(groupId, children);
                that.emit('consumersChanged');
            }
        }
    );
};

Zookeeper.prototype.dataOfConsumers = function (groupId, consumers) {
    var that = this;
    var path = '/consumers/' + groupId + '/ids/';
    if (typeof consumers === 'string') {
        consumers = [consumers];
    }
    if (typeof consumers === 'object' && consumers.length) {
        consumers.forEach(function (consumer) {
            if (!consumersInList.hasOwnProperty(consumer)) {
                consumersInList[consumer] = true;
                that.client.getData(
                    path + consumer,
                    function (event) {
                        that.emit('consumersChanged');
                        that.dataOfConsumers(groupId, consumer);
                    },
                    function (error, data, stat) {
                        if (error) {
                            delete consumersInList[consumer];
                            that.emit('error', error);
                        }
                    }
                );
            }
        });
    }
};

Zookeeper.prototype.topicExists = function (topic, cb, watch) {
    var path = '/brokers/topics/' + topic,
        self = this;
    this.client.exists(
        path,
        function (event) {
            debug('Got event: %s.', event);
            if (watch)
                self.topicExists(topic, cb);
        },
        function (error, stat) {
            if (error) return;
            cb(!!stat, topic);
        }
    );
}

Zookeeper.prototype.deletePartitionOwnership = function (groupId, topic, partition, cb) {
    var path = '/consumers/' + groupId + '/owners/' + topic + '/' + partition,
        self = this;
    this.client.remove(
        path,
        function (error) {
            if (error)
                cb(error);
            else {
                debug("Removed partition ownership %s", path);
                cb();
            }
        }
    );
}

Zookeeper.prototype.addPartitionOwnership = function (consumerId, groupId, topic, partition, cb) {
    var path = '/consumers/' + groupId + '/owners/' + topic + '/' + partition,
        self = this;

    async.series([
            function (callback) {
                self.client.mkdirp(
                        '/consumers/' + groupId + '/owners/' + topic,
                    function (error, path) {
                        // simply carry on
                        callback();
                    });
            },
            function (callback) {
                self.client.create(
                    path,
                    new Buffer(consumerId),
                    null,
                    zookeeper.CreateMode.EPHEMERAL,
                    function (error, path) {
                        if (error) {
                            callback(error);
                        }
                        else callback();
                    });
            },
            function (callback) {
                self.client.exists(path, null, function (error, stat) {
                    if (error) {
                        callback(error);
                    }
                    else if (stat) {
                        debug('Gained ownership of %s by %s.', path, consumerId);
                        callback();
                    }
                    else {
                        callback("Path wasn't created");
                    }
                });
            }],
        function (err) {
            if (err) cb(err);
            else cb();
        });
}


Zookeeper.prototype.checkPartitionOwnership = function (consumerId, groupId, topic, partition, cb) {
    var path = '/consumers/' + groupId + '/owners/' + topic + '/' + partition,
        self = this;

    async.series([
            function (callback) {
                self.client.exists(path, null, function (error, stat) {
                    if (error) {
                        callback(error);
                    }
                    else if (stat) {
                        callback();
                    }
                    else {
                        callback("Path wasn't created");
                    }
                });
            },
            function (callback) {
                self.client.getData(
                    path,
                    function (error, data) {
                        if (error) {
                            callback(error);
                        }
                        else {
                            if (consumerId !== data.toString())
                                callback("Consumer not registered " + consumerId);
                            else
                                callback();
                        }
                    }
                );
            }],
        function (err) {
            if (err) cb(err);
            else cb();
        });
}

var ZookeeperConsumerMappings = function () {
    this.consumerTopicMap = {};
    this.topicConsumerMap = {};
    this.topicPartitionMap = {};
};


exports.Zookeeper = Zookeeper;
exports.ZookeeperConsumerMappings = ZookeeperConsumerMappings;
