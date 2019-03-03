var REQ_COUNT = 50000;

var Test = {
    map: undefined,
    finishCallback: undefined,
    ops: 0,
    increment: function () {
        this.ops = this.ops + 1;
        if (this.ops === REQ_COUNT) {
            var date = new Date();
            this.run = function () {
            };
            this.finishCallback(date);
        }
    },
    run: function () {
        this.map.put('foo', 'bar').then(this.increment.bind(this));
        setImmediate(this.run.bind(this));
    }
};
var Client = require('../.').Client;
var hazelcastClient;

Client.newHazelcastClient().then(function (client) {
    hazelcastClient = client;
    return hazelcastClient.getMap('default');
}).then(function (mp) {
    Test.map = mp;

    var start;
    Test.finishCallback = function (finish) {
        console.log('Took ' + (finish - start) / 1000 + ' seconds for ' + REQ_COUNT + ' requests');
        console.log('Ops/s: ' + REQ_COUNT / ((finish - start) / 1000));
        Test.map.destroy().then(function () {
            hazelcastClient.shutdown();
        });
    };
    start = new Date();
    Test.run();
});
