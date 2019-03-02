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

class Benchmark {
    constructor(map) {
        this.map = map;
        this.ops = 0;
    }
    // increments ops counter, creates a new op and returns it
    _nextOp() {
        this.ops++;
        const key = Math.random() * ENTRY_COUNT;
        const opType = Math.floor(Math.random() * 100);
        if (opType < GET_PERCENTAGE) {
            return this.map.get(key);
        } else if (opType < GET_PERCENTAGE + PUT_PERCENTAGE) {
            return this.map.put(key, value_string);
        } else {
            return this.map.remove(key);
        }
    }
    // chains next op once one of ops finishes to keep constant concurrency of ops
    _chainNext(p) {
        return p.then(() => {
            if (this.ops < REQ_COUNT) {
                return this._chainNext(this._nextOp());
            }
        });
    }
    run() {
        // initial batch of ops (no-op promises)
        const batch = new Array(BATCH_SIZE).fill(Promise.resolve());
        const start = new Date();
        return Promise.all(batch.map(this._chainNext.bind(this)))
            .then(() => {
                const finish = new Date();
                const tookSec = (finish - start) / 1000;
                console.log(`Took ${tookSec} seconds for ${this.ops} requests`);
                console.log(`Ops/s: ${this.ops / tookSec}`);
            });
    }
};

const Client = require('../.').Client;

Client.newHazelcastClient()
    .then((client) => client.getMap('default'))
    .then((map) => {
        const benchmark = new Benchmark(map);
        return benchmark.run()
            .then(() => map.destroy())
            .then(() => map.client.shutdown());
    })
    .then(() => console.log('Benchmark finished'));
