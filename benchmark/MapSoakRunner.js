'use strict';

const Benchmark = require('./SimpleBenchmark');
const Client = require('../.').Client;
const Config = require('../.').Config;

const IdentifiedFactory = require('../test/javaclasses/IdentifiedFactory');
const IdentifiedEntryProcessor = require('../test/javaclasses/IdentifiedEntryProcessor');

/**
 * Entry listener
 */

function nop() {

}

var listener = {
    added: function (key, oldvalue, value, mergingvalue) {
        nop(key, oldvalue, value, mergingvalue);
    },
    updated: function (key, oldvalue, value, mergingvalue) {
        nop(key, oldvalue, value, mergingvalue);
    },
    removed: function (key, oldvalue, value, mergingvalue) {
        nop(key, oldvalue, value, mergingvalue);
    },
    evicted: function (key, oldvalue, value, mergingvalue) {
        nop(key, oldvalue, value, mergingvalue);
    },
    clearedAll: function (key, oldvalue, value, mergingvalue) {
        nop(key, oldvalue, value, mergingvalue);
    },
    evictedAll: function (key, oldvalue, value, mergingvalue) {
        nop(key, oldvalue, value, mergingvalue);
    },
};

function randomString(max) {
    return Math.floor(Math.random() * Math.floor(max)).toString();
}

function randomInt(upto) {
    return Math.floor(Math.random() * upto);
}

function randomOp(map) {
    var key = randomString(10000);
    var value = randomString(10000);
    var operation = 60; //randomInt(100);

    if (operation < 30) {
        return map.get(key);
    } else if (operation < 80) {
        return map.put(key, value);
    } else if (operation < 80) {
        return map.values(Predicates.isBetween('this', 0, 10));
    } else {
        return map.executeOnKey(key, new IdentifiedEntryProcessor(key));
    }
}

const cfg = new Config.ClientConfig();
for (let i = 2; i < process.argv.length; i++) {
    cfg.networkConfig.addresses[0] = process.argv[i];
}
cfg.serializationConfig.dataSerializableFactories[66] = new IdentifiedFactory();

Client.newHazelcastClient(cfg).then(function (client) {
    return client.getMap('default');
}).then(function (map) {
    map.addEntryListener(listener);
    return map;
})
.then((map) => {
    const benchmark = new Benchmark({
        nextOp: () => randomOp(map),
        totalOpsCount: 50000,
        batchSize: 32
    });
    return benchmark.run()
        .then(() => map.destroy())
        .then(() => map.client.shutdown());
})
.then(() => console.log('Benchmark finished'));

