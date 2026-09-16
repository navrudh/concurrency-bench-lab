"""
Adaptive Client with CoDel (Controlled Delay) Algorithm & Header Pacing
========================================================================
Next-generation client mechanism:
Rather than hardcoding concurrency caps or waiting for 429 errors,
this client monitors moving queue delay (CoDel) and automatically paces
outbound request rate via adaptive token buckets.

Usage:
  python3 benchmarks/forward_adaptive_codel_client.py --url http://127.0.0.1:9090/work --requests 1000
"""

import argparse
import asyncio
import time
from typing import Tuple

class CoDelPacer:
    """
    CoDel (Controlled Delay) rate limiter.
    Maintains target queue delay (e.g. 15ms). If minimum observed latency
    exceeds target over an interval (e.g. 100ms), drops in-flight budget.
    """
    def __init__(self, target_ms: float = 25.0, interval_ms: float = 100.0, initial_concurrency: int = 50):
        self.target_ms = target_ms
        self.interval_ms = interval_ms
        self.concurrency = initial_concurrency
        self.min_concurrency = 5
        self.max_concurrency = 500

        self.interval_start = time.perf_counter()
        self.interval_min_latency = float("inf")
        self.dropping_state = False

    def record_latency(self, latency_ms: float):
        now = time.perf_counter()
        if latency_ms < self.interval_min_latency:
            self.interval_min_latency = latency_ms

        elapsed_ms = (now - self.interval_start) * 1000.0
        if elapsed_ms >= self.interval_ms:
            # Evaluate whether min latency exceeded target
            if self.interval_min_latency > self.target_ms:
                if not self.dropping_state:
                    self.dropping_state = True
                # Multiplicative decrease
                self.concurrency = max(self.min_concurrency, int(self.concurrency * 0.75))
            else:
                self.dropping_state = False
                # Additive increase
                self.concurrency = min(self.max_concurrency, self.concurrency + 5)

            # Reset interval
            self.interval_start = now
            self.interval_min_latency = float("inf")

async def make_async_raw_request(host: str, port: int, path: str, timeout: float = 5.0) -> Tuple[int, float]:
    start = time.perf_counter()
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
        req = f"GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\n\r\n"
        writer.write(req.encode("latin1"))
        await writer.drain()

        line = await asyncio.wait_for(reader.readline(), timeout=timeout)
        elapsed = (time.perf_counter() - start) * 1000.0
        parts = line.decode("latin1", errors="replace").split(" ")
        status = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0

        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return status, elapsed
    except Exception:
        elapsed = (time.perf_counter() - start) * 1000.0
        return 0, elapsed

async def run_codel_client(host: str, port: int, path: str, total_requests: int):
    pacer = CoDelPacer(target_ms=30.0, interval_ms=50.0, initial_concurrency=40)
    sem = asyncio.Semaphore(pacer.concurrency)

    start_all = time.perf_counter()
    success_count = 0
    shed_count = 0

    async def worker():
        nonlocal success_count, shed_count
        async with sem:
            status, latency = await make_async_raw_request(host, port, path)
            pacer.record_latency(latency)
            if status == 200:
                success_count += 1
            elif status == 429:
                shed_count += 1
                pacer.concurrency = max(5, int(pacer.concurrency * 0.5))

    tasks = [asyncio.create_task(worker()) for _ in range(total_requests)]
    await asyncio.gather(*tasks)

    duration = time.perf_counter() - start_all
    tps = total_requests / duration if duration > 0 else 0
    print(f"\n[CoDel Adaptive Client Summary]")
    print(f"Total Requests:      {total_requests}")
    print(f"Elapsed Time:        {duration:.2f}s")
    print(f"Effective TPS:       {tps:.1f}")
    print(f"Successful:          {success_count} ({(success_count/total_requests)*100:.1f}%)")
    print(f"Shed (429):          {shed_count} ({(shed_count/total_requests)*100:.1f}%)")
    print(f"Final Concurrency:   {pacer.concurrency}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9090)
    parser.add_argument("--path", default="/work")
    parser.add_argument("--requests", type=int, default=500)
    args = parser.parse_args()

    asyncio.run(run_codel_client(args.host, args.port, args.path, args.requests))
