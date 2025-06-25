"""
Beginner Migration & Scaling Benchmark
=====================================
Journey of a developer migrating from sync (requests/urllib) to async (asyncio):
- Experiment Set 1: How does a beginner try to scale up sync?
  1. Sequential (1 request at a time) -> Painfully slow.
  2. ThreadPoolExecutor (10, 50, 100 threads) -> Hits OS thread wall & GIL.
  3. First AsyncIO script -> Unbounded coroutines!

- Experiment Set 2: The Curious Cat Scaling & Server Tuning:
  What happens when the client goes to the extreme (500 to 2,000 requests)?
  What server tunings can we do so the server handles as many as possible?
  Tuning 1: Default baseline server
  Tuning 2: Increase concurrency limit (workers)
  Tuning 3: Increase buffer/queue capacity
  Tuning 4: Reduce simulated I/O latency (faster downstream / optimized pipeline)
"""

import argparse
import asyncio
import concurrent.futures
import statistics
import time
import urllib.request
from typing import List, Tuple

def make_sync_request(url: str, timeout: float = 10.0) -> Tuple[int, float]:
    start = time.perf_counter()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "BenchSync/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            elapsed = (time.perf_counter() - start) * 1000.0
            return resp.status, elapsed
    except urllib.error.HTTPError as e:
        elapsed = (time.perf_counter() - start) * 1000.0
        return e.code, elapsed
    except Exception:
        elapsed = (time.perf_counter() - start) * 1000.0
        return 0, elapsed

async def make_async_request(host: str, port: int, path: str, timeout: float = 10.0) -> Tuple[int, float]:
    start = time.perf_counter()
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
        req = f"GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\nUser-Agent: BenchAsync/1.0\r\n\r\n"
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

def run_sync_bench(name: str, url: str, total_requests: int, max_workers: int) -> dict:
    start_total = time.perf_counter()
    latencies = []
    status_counts = {}

    if max_workers == 1:
        # Pure sequential
        for _ in range(total_requests):
            code, elapsed = make_sync_request(url)
            latencies.append(elapsed)
            status_counts[code] = status_counts.get(code, 0) + 1
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(make_sync_request, url) for _ in range(total_requests)]
            for f in concurrent.futures.as_completed(futures):
                code, elapsed = f.result()
                latencies.append(elapsed)
                status_counts[code] = status_counts.get(code, 0) + 1

    total_time = time.perf_counter() - start_total
    return format_stats(name, total_requests, total_time, latencies, status_counts)

async def run_async_bench(name: str, host: str, port: int, path: str, total_requests: int, concurrency: int) -> dict:
    start_total = time.perf_counter()
    latencies = []
    status_counts = {}

    sem = asyncio.Semaphore(concurrency)

    async def worker():
        async with sem:
            return await make_async_request(host, port, path)

    tasks = [asyncio.create_task(worker()) for _ in range(total_requests)]
    results = await asyncio.gather(*tasks)

    for code, elapsed in results:
        latencies.append(elapsed)
        status_counts[code] = status_counts.get(code, 0) + 1

    total_time = time.perf_counter() - start_total
    return format_stats(name, total_requests, total_time, latencies, status_counts)

def format_stats(name: str, total: int, total_time: float, latencies: List[float], status_counts: dict) -> dict:
    latencies.sort()
    p50 = statistics.median(latencies) if latencies else 0
    p95 = latencies[int(len(latencies) * 0.95)] if latencies else 0
    p99 = latencies[int(len(latencies) * 0.99)] if latencies else 0
    tps = total / total_time if total_time > 0 else 0

    success = status_counts.get(200, 0)
    shed = status_counts.get(429, 0) + status_counts.get(503, 0)

    return {
        "name": name,
        "total": total,
        "time_sec": round(total_time, 2),
        "tps": round(tps, 1),
        "p50_ms": round(p50, 1),
        "p95_ms": round(p95, 1),
        "p99_ms": round(p99, 1),
        "success": success,
        "success_pct": round((success / total) * 100, 1),
        "shed": shed,
    }

def print_table(title: str, rows: List[dict]):
    print(f"\n{title}")
    print(f"{'EXPERIMENT':<42} | {'TPS':<7} | {'p50(ms)':<8} | {'p99(ms)':<8} | {'SUCCESS':<12} | {'TOTAL TIME'}")
    print("-" * 96)
    for r in rows:
        print(f"{r['name']:<42} | {r['tps']:<7} | {r['p50_ms']:<8} | {r['p99_ms']:<8} | {r['success']}/{r['total']} ({r['success_pct']}%) | {r['time_sec']}s")
    print("-" * 96)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["set1", "set2", "all"], default="all")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--requests", type=int, default=200)
    args = parser.parse_args()

    url = f"http://{args.host}:{args.port}/work"

    if args.mode in ["set1", "all"]:
        # EXPERIMENT SET 1: The Beginner's Journey from Sync to Async
        print("\n" + "="*80)
        print("EXPERIMENT SET 1: A Beginner's Journey to Scale Throughput (Sync -> Async)")
        print("="*80)
        s1_rows = []

        # 1. Sequential (1 request at a time)
        print("-> Running [1/4] Naive Sequential Sync (1 by 1)...")
        r1 = run_sync_bench("1. Naive Sequential (1 by 1)", url, min(args.requests, 50), max_workers=1)
        s1_rows.append(r1)

        # 2. ThreadPool with 10 threads
        print("-> Running [2/4] ThreadPoolExecutor (10 threads)...")
        r2 = run_sync_bench("2. ThreadPool (10 workers)", url, args.requests, max_workers=10)
        s1_rows.append(r2)

        # 3. ThreadPool with 50 threads
        print("-> Running [3/4] ThreadPoolExecutor (50 threads)...")
        r3 = run_sync_bench("3. ThreadPool (50 workers - thread wall)", url, args.requests, max_workers=50)
        s1_rows.append(r3)

        # 4. Naive AsyncIO (100 in-flight coroutines)
        print("-> Running [4/4] First AsyncIO Script (100 coroutines)...")
        r4 = asyncio.run(run_async_bench("4. First AsyncIO (100 coroutines)", args.host, args.port, "/work", args.requests, concurrency=100))
        s1_rows.append(r4)

        print_table("RESULTS: Experiment Set 1 (Sync vs Async Scaling)", s1_rows)

    if args.mode in ["set2", "all"]:
        # EXPERIMENT SET 2: The Curious Cat - Scaling Client to Extreme & Server Tuning
        print("\n" + "="*80)
        print("EXPERIMENT SET 2: The Curious Cat - Extreme Client Load (500 Coroutines)")
        print("="*80)
        s2_rows = []

        print("-> Testing Extreme Async Client (500 concurrent coroutines)...")
        r_extreme = asyncio.run(run_async_bench("Extreme AsyncIO (500 coroutines)", args.host, args.port, "/work", total_requests=1000, concurrency=500))
        s2_rows.append(r_extreme)

        print_table("RESULTS: Experiment Set 2 (Extreme Client Under Current Server)", s2_rows)

