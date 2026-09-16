# Concurrency Bench Lab

A small playground and benchmark setup to test what actually happens when you try to scale up Python HTTP clients from standard synchronous threads to `asyncio`, and how servers hold up when flooded with requests.

---

## The Story

When an API client or data fetch script is running too slow, the first instinct is always: *"just throw a thread pool at it."* 

It works great at first, but soon hits a wall where adding more threads doesn't help and CPU usage starts shooting up. 

This repo tests that transition step-by-step:
1. **Sequential calls** (one by one, painfully slow).
2. **ThreadPoolExecutor** with 10, 50, and 100 workers (where thread contention kicks in).
3. **AsyncIO** using a single event loop.
4. **Server tunings** to see how expanding worker capacity vs. queue buffers keeps the server from dropping requests under a heavy burst.

---

## Benchmark Numbers

Tested locally against a mock server with a 20ms simulated downstream delay. Each test was repeated 3 times and averaged:

### Experiment 1: Scaling the Client (Sync vs Async)

| Approach | Throughput | Client CPU | p50 Latency | p99 Latency | Success | Notes |
|---|---|---|---|---|---|---|
| **Sequential (1 by 1)** | ~45 req/s | ~5% | 22 ms | 28 ms | 100% | CPU sits idle waiting on network |
| **ThreadPool (10 workers)** | ~416 req/s | ~38% | 23 ms | 27 ms | 100% | Clean 10x linear speedup |
| **ThreadPool (50 workers)** | ~1,221 req/s | ~115% | 28 ms | 44 ms | 100% | Works well, but burns full core |
| **ThreadPool (100 workers)** | ~1,123 req/s | ~114% | 30 ms | 48 ms | 100% | **Hits the wall:** throughput drops due to context switching & GIL overhead |
| **AsyncIO (100 coroutines)** | **~1,813 req/s** | **~47%** | 40 ms | 60 ms | 100% | **60% faster than threads, uses less than half the CPU** |

### Experiment 2: Pushing to the Extreme (500 Concurrent Coroutines)

What happens when an async client fires 500 requests at once into the server?

| Server Setup | Throughput | Server Drops (429) | Success Rate | What Happens |
|---|---|---|---|---|
| **Baseline Server** (50 workers, 200 queue) | ~2,713 req/s | 53.5% dropped | 46.5% | Server runs out of buffer room and sheds the burst |
| **Tuned Server** (200 workers, 1000 queue) | **~3,483 req/s** | **0% dropped** | **100.0%** | Handled all 1,000 requests smoothly without breaking a sweat |

---

## What I Learned

1. **Thread pools are good, until they aren't:** Going from 1 to 50 threads gives an easy 25x boost. But pushing to 100 threads actually made performance worse (~1,220 down to ~1,120 req/s) because Python threads spend more time fighting over the GIL and switching context than doing useful work.
2. **AsyncIO is about CPU efficiency:** The async client ran at ~1,800 req/s while only using ~47% CPU. The thread pool burned over 114% CPU to do less work.
3. **If you send a firehose, the server needs buffer room:** When you switch to async, the client fires so fast that the downstream server will immediately drop requests with `429` unless its queue and worker pool are sized to absorb the spike.

---

## How to Run It

### 1. Start the mock server
```bash
python3 server-python/mock_server.py --port 9090 --concurrency 50 --queue-cap 200 --delay-ms 20
```

### 2. Run the benchmark with CPU tracking
```bash
python3 benchmarks/benchmark_with_cpu.py --port 9090 --requests 200 --runs 3
```

*(There is also an ultra-fast Rust Axum server in `server-rust/` for testing higher limits if you have Rust installed or use container builds).*
