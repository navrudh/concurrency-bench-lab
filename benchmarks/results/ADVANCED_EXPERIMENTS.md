# Advanced Experiments Laboratory Log: Keep-Alive, Adaptive Concurrency, and Backoff Dynamics

**Date of Run:** 2026-09-16
**Target Repo:** `navrudh/concurrency-bench-lab`

---

## 1. Context & Evolving Methodology

Our previous experiments (`OBSERVATIONS.md`) exposed that thread-pool exhaustion artificially throttles clients, whereas event-loop concurrency (AsyncIO) can instantly overwhelm server buffers (the Buffer Squeeze).
However, those early tests suffered from **connection setup overhead conflation**: each request opened a new socket. 
This new suite isolates *pure network I/O concurrency* by implementing **HTTP Keep-Alive (Connection Pooling)** across both Threaded and Async paradigms.

Furthermore, we introduced two crucial architectural patterns to handle buffer overflow (429 Load Shedding):
1. **Adaptive Concurrency Limits (TCP Vegas / Netflix style):** Dynamically growing/shrinking client concurrency based on observed latency (Little's Law applied at the edge).
2. **Exponential Backoff with Jitter:** Reacting to 429s by pacing retries to spread the burst and avoid retry storms.

---

## 2. Experimental Setup

### Server Parameters
- **Port:** `8080` (Python Mock Server)
- **Active Worker Capacity:** `30` concurrent slots
- **Admission Queue Buffer:** `100` slots
- **Simulated Latency:** `20 ms` per active work item
- **Admission Threshold:** Max concurrent requests in flight = 130

### Client Parameters
- **Requests:** 1000
- **Max Concurrency Cap:** 250

---

## 3. Results & Empirical Analysis

```text
===============================================================================================
ENGINE                              | TPS      | p50(ms)  | p99(ms)  | SUCCESS  | SHED(429)
===============================================================================================
Threaded Keep-Alive                 | 1332.32  | 34.85    | 47.13    | 100.0%   | 0 (0.0%)
AsyncIO Keep-Alive                  | 5607.82  | 35.23    | 143.02   | 16.0%    | 840 (84.0%)
AsyncIO Adaptive Concurrency (Vegas) | 1300.74  | 70.54    | 97.04    | 98.6%    | 14 (1.4%)
AsyncIO Exp Backoff + Jitter        | 539.23   | 62.73    | 132.45   | 100.0%   | 0 (0.0%)
===============================================================================================
```

### Observation 1: Keep-Alive Exposes True Asynchronous Firepower
By reusing TCP connections, the `AsyncIO Keep-Alive` client was able to blast through the test at **5,607 TPS** (compared to ~3,363 TPS in the old raw-socket benchmark).
Because it sent all 250 requests instantly across pooled connections, it massively overflowed the server's 130-slot limit, resulting in **84% of requests being instantly shed (429s)**.

### Observation 2: The Thread Pool Continues to Self-Throttle
Even with connection pooling, `Threaded Keep-Alive` achieved 100% success. Why? Because the thread pool was capped at 50 workers, and it physically could not generate enough concurrent in-flight requests to exceed the server's 130-slot capacity. It acts as a static, but unintentional, client-side limit.

### Observation 3: Adaptive Concurrency Limit (The Sweet Spot)
Our Vegas-style client dynamically adjusted its in-flight limit based on latency. 
- It achieved nearly the same throughput as the thread pool (**1,300 TPS**) but dynamically self-regulated to prevent overwhelming the server.
- **Result:** Shedding dropped drastically to **1.4%**. It successfully navigated the server's buffer limits without requiring manual tuning of static thread counts.

### Observation 4: Exponential Backoff & Jitter (The Safety Net)
When clients react to 429s with jittered exponential backoff:
- **Success recovered to 100%**. 
- However, overall **Throughput dropped (539 TPS)** and tail latencies grew (since requests slept before retrying).
- **Takeaway:** Backoff is vital for reliability, but relying purely on backoff after shedding is less efficient than *avoiding* shedding in the first place via Adaptive Concurrency Limits.

---

## 4. Architectural Conclusions

To eliminate thread contention and optimize distributed system throughput:
1. **Move to AsyncIO + Connection Pooling** to maximize network I/O efficiency and save host CPU/memory.
2. **Implement Client-Side Adaptive Concurrency Limits.** Do not blast endless coroutines. Regulate in-flight async requests dynamically based on observed latency or explicit backpressure.
3. **Configure Server-Side Strict Buffers.** Bound your server queues and shed load quickly with `429`s to protect active p50 latency.
4. **Use Jittered Backoff as a Fallback.** When shedding does occur, clients must retreat probabilistically to avoid thundering herds.
