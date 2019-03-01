const REQ_COUNT = 50000;
const BATCH_SIZE = 100;
const ENTRY_COUNT = 10 * 1000;
const VALUE_SIZE = 10000;
const GET_PERCENTAGE = 40;
const PUT_PERCENTAGE = 40;

let value_string = '';
for (let i = 0; i < VALUE_SIZE; i++) {
    value_string = value_string + 'x';
}

const Test = {
    map: undefined,
    finishCallback: undefined,
    ops: 0,
    increment: function () {
        this.ops = this.ops + 1;
        if (this.ops === REQ_COUNT) {
            this.run = () => {};
            this.finishCallback(new Date());
        }
    },
    run: function () {
        const batch = [];
        for (let i = 0; i < BATCH_SIZE; i++) {
            const key = Math.random() * ENTRY_COUNT;
            const opType = Math.floor(Math.random() * 100);
            if (opType < GET_PERCENTAGE) {
                batch.push(this.map.get(key).then(this.increment.bind(this)));
            } else if (opType < GET_PERCENTAGE + PUT_PERCENTAGE) {
                batch.push(this.map.put(key, value_string).then(this.increment.bind(this)));
            } else {
                batch.push(this.map.remove(key).then(this.increment.bind(this)));
            }
        }
        Promise.all(batch).then(this.run.bind(this));
    }
};

const Client = require('../.').Client;
let hazelcastClient;

Client.newHazelcastClient().then((client) => {
    hazelcastClient = client;
    return hazelcastClient.getMap('default');
}).then((map) => {
    Test.map = map;
    const start = new Date();
    Test.finishCallback = (finish) => {
        console.log(`Took ${(finish - start) / 1000} seconds for ${REQ_COUNT} requests`);
        console.log(`Ops/s: ${REQ_COUNT / ((finish - start) / 1000)}`);
        Test.map.destroy().then(() => {
            hazelcastClient.shutdown();
        });
    };
    Test.run();
});
