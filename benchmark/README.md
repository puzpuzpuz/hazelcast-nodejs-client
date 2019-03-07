# Simple benchmark for Hazelcast IMDG Node.js Client

This simple benchmark runs `map.put('foo', 'bar')` operations in parallel, measures execution time and calculates throughput.

## Running the benchmark

First, install dependencies and build the client:
```bash
npm install
```

Then, build the client (compile TypeScript):
```bash
npm run compile
```

Next, run at least one instance of IMDG. The most simple way to do it would be to use the [official Docker image](https://hub.docker.com/r/hazelcast/hazelcast/):
```bash
docker run -p 5701:5701 hazelcast/hazelcast:3.11.2
```

Finally, run the benchmark:
```bash
node benchmark/SimpleMapBenchmark.js
```

The benchmark will run and produce its results into the console:
```bash
[DefaultLogger] INFO at ConnectionAuthenticator: Connection to 172.17.0.2:5701 authenticated
[DefaultLogger] INFO at ClusterService: Members received.
[ Member {
    address: Address { host: '172.17.0.2', port: 5701, type: 4 },
    uuid: '1c07e7a7-5301-452c-b098-3e662126e4fe',
    isLiteMember: false,
    attributes: {} } ]
[DefaultLogger] INFO at HazelcastClient: Client started
Took 0.855 seconds for 50000 requests
Ops/s: 58479.53216374269
Benchmark finished
```
