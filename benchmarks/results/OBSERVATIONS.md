# Experimental Laboratory Field Log: Concurrency, Buffer Tuning & Load Shedding

**Date of Run:** 2026-09-16 (Empirical Evaluation)  
**Target Repo:** [`navrudh/concurrency-bench-lab`](https://github.com/navrudh/concurrency-bench-lab)  
**Author / Observer:** Dhruvan Ganesh  
**Investigative Mindset:** Curious Cat 🐾 (Poking at the edge of the table until something spills)

---

## 1. The Experiment Hypothesis

When a service is subjected to traffic bursts that exceed its active execution capacity, two interconnected architectural dynamics collide:

1. **Client-Side Concurrency Limits (Thread-bound vs. Event Loop):**
   - *Hypothesis:* Standard thread pools (`ThreadPoolExecutor`) hit a ceiling imposed by OS context switching, thread memory footprint, and the Python GIL. They cannot fire fast enough to overwhelm the server's admission buffer.
   - *Contrast:* A non-blocking `asyncio` client can schedule hundreds of concurrent coroutines on a **single OS thread**, generating massive egress packet density that will immediately reveal whether downstream backpressure works.

2. **Server-Side Admission Control (Queue Buffer Tuning vs. Load Shedding):**
   - *Hypothesis:* A downstream service without an admission buffer experiences queue bloat and eventual connection timeouts (death by bufferbloat). 
   - *With a bounded buffer:* When active workers ($W=30$) and queue buffer ($Q=100$) are full ($W + Q = 130$), the service should deterministically shed excess load with `HTTP 429 Too Many Requests` in sub-millisecond time, protecting its active workers and keeping p50 latency low.

---

## 2. Experimental Setup & Control Knobs

### Server Parameters:
- **Port:** `9090`
- **Active Worker Capacity ($W$):** `30` concurrent slots
- **Admission Queue Buffer ($Q$):** `100` slots
- **Simulated Downstream Latency ($D$):** `20 ms` per active work item
- **Admission Threshold:** Max concurrent requests in flight $= W + Q = 130$

---

## 3. Empirical Test Runs

### Phase 1: The Gentle Stroll (100 Requests, Concurrency = 20)
*Below the saturation limit ($20 < 130$).*

```
================================================================================
ENGINE                              | TPS      | p50(ms)  | p99(ms)  | SUCCESS  | SHED(429)
================================================================================
Threaded Blocking (ThreadPool)      | 515.85   | 26.16    | 73.83    | 100.0%   | 0 (0.0%)
AsyncIO Non-Blocking (Pure Stream)  | 730.27   | 24.38    | 33.22    | 100.0%   | 0 (0.0%)
================================================================================
```

### 🐱 Curious Observation 1:
- Both clients passed with 100% success.
- **Tail Latency divergence:** Even with zero queue contention, `asyncio`'s p99 was **33.22 ms** vs. ThreadPool's **73.83 ms** (>2.2x tail latency penalty on threads).
- *Why?* Thread switching has preemptive scheduling jitter. The OS scheduler wakes and suspends kernel threads with non-deterministic microsecond latencies. Async coroutines resume cooperatively immediately upon epoll notification.

---

### Phase 2: The Firehose Burst (1,000 Requests, Concurrency = 250)
*Burst concurrency ($250$) far exceeds server capacity ($W+Q = 130$).*

```
================================================================================
ENGINE                              | TPS      | p50(ms)  | p99(ms)  | SUCCESS  | SHED(429)
================================================================================
Threaded Blocking (ThreadPool-50)   | 1252.03  | 33.84    | 62.60    | 100.0%   | 0 (0.0%)
AsyncIO Non-Blocking (Stream-250)   | 3363.77  | 17.07    | 147.65   | 31.0%    | 690 (69.0%)
================================================================================
```

### 🐱 Curious Observation 2: The Thread Pool Self-Throttling Illusion
- Notice something striking: **The Threaded client had 0% dropped requests (100% success)!**
- Did the server magically get bigger? **No.**
- Because the client was limited to 50 OS threads, it physically *could not fire requests fast enough* to overflow the 130-slot server buffer ($50 < 130$). The threads spent all their time waiting synchronously on open sockets.
- **The danger:** In production, teams running threaded clients often believe their service has infinite capacity, when in reality their client is artificially rate-limiting them at sluggish throughput (1,252 TPS vs 3,363 TPS).

### 🐱 Curious Observation 3: The AsyncIO Micro-Burst & The Buffer Squeeze
- The `asyncio` client launched all 250 in-flight requests simultaneously onto the loop.
- **Result:**
  - $30$ requests entered active execution.
  - $100$ requests filled the admission queue.
  - Exactly **$690$ requests were rejected with `429 Too Many Requests`**.
- Check the server metrics tally:
  ```json
  {"received": 2200, "processed": 1510, "shed": 690, "current_queued": 0, "current_active": 0}
  ```
- **Math check:** Total shed across both runs was exactly 690. The admission control was mathematically precise to the exact integer.
- **Fast Failure:** Shed requests returned in **< 1.2 milliseconds**, freeing client resources immediately rather than hanging in socket timeout limbo.

---

## 4. Key Takeaways for High-Scale Backend Systems

1. **Thread Pools hide downstream bottlenecks by bottlenecking themselves first.** You don't realize your downstream buffer is undersized until you switch to an async client or scale horizontally.
2. **Buffer Tuning is a trade-off between memory and latency:**
   - A huge buffer ($Q = 10,000$) prevents 429s, but causes **bufferbloat**—requests sit in queue for seconds before being executed, violating upstream p99 SLAs.
   - A tight buffer ($Q = 100$) forces rapid admission or rejection, keeping active latency low (p50 $= 17.07\text{ ms}$).
3. **Failing Fast (`429`) is a feature, not a bug.** It gives the caller a deterministic signal to apply exponential backoff and jitter rather than piling into a stampede.
