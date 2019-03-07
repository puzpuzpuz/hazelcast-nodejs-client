const REQ_COUNT = 50000;
const BATCH_SIZE = 100;

class Benchmark {
    constructor(map) {
        this.map = map;
        this.ops = 0;
    }
    // increments ops counter, creates a new op and returns it
    _nextOp() {
        this.ops++;
        return this.map.put('foo', 'bar');
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
