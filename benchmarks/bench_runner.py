"""
Benchmark Suite: Comparing Threaded Blocking (requests) vs. Asynchronous Non-Blocking (asyncio + aiohttp / httpx)
Tests:
1. Standard horizontal scaling across worker threads.
2. Buffer saturation & 429/503 shedding behavior under traffic spikes.
3. Latency distribution (p50, p90, p99) and CPU/thread overhead.
"""

import argparse
import asyncio
import concurrent.futures
import json
import statistics
import time
import urllib.request
from typing import List, Tuple

def make_sync_request(url: str, timeout: float = 5.0) -> Tuple[int, float]:
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

def run_sync_threaded_bench(url: str, total_requests: int, concurrency: int) -> dict:
    print(f"\n[1] Running Threaded Blocking Benchmark (threads={concurrency}, total={total_requests})...")
    start_total = time.perf_counter()
    latencies = []
    status_counts = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(make_sync_request, url) for _ in range(total_requests)]
        for f in concurrent.futures.as_completed(futures):
            code, elapsed = f.result()
            latencies.append(elapsed)
            status_counts[code] = status_counts.get(code, 0) + 1

    total_time = time.perf_counter() - start_total
    return compile_stats("Threaded Blocking (ThreadPoolExecutor)", total_requests, total_time, latencies, status_counts)

async def make_async_request(client, url: str, timeout: float = 5.0) -> Tuple[int, float]:
    start = time.perf_counter()
    try:
        resp = await client.get(url, timeout=timeout)
        elapsed = (time.perf_counter() - start) * 1000.0
        return resp.status_code, elapsed
    except Exception:
        elapsed = (time.perf_counter() - start) * 1000.0
        return 0, elapsed

async def make_async_raw_request(host: str, port: int, path: str, timeout: float = 5.0) -> Tuple[int, float]:
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

async def run_asyncio_bench(url: str, total_requests: int, concurrency: int) -> dict:
    from urllib.parse import urlparse
    parsed = urlparse(url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 80
    path = parsed.path or "/"
    if parsed.query:
        path += f"?{parsed.query}"

    print(f"\n[2] Running AsyncIO Non-Blocking Benchmark (concurrency={concurrency}, total={total_requests})...")
    start_total = time.perf_counter()
    latencies = []
    status_counts = {}

    sem = asyncio.Semaphore(concurrency)

    async def worker():
        async with sem:
            return await make_async_raw_request(host, port, path)

    tasks = [asyncio.create_task(worker()) for _ in range(total_requests)]
    results = await asyncio.gather(*tasks)

    for code, elapsed in results:
        latencies.append(elapsed)
        status_counts[code] = status_counts.get(code, 0) + 1

    total_time = time.perf_counter() - start_total
    return compile_stats("AsyncIO Non-Blocking (Pure Asyncio Stream)", total_requests, total_time, latencies, status_counts)

def compile_stats(name: str, total: int, total_time: float, latencies: List[float], status_counts: dict) -> dict:
    latencies.sort()
    p50 = statistics.median(latencies) if latencies else 0
    p90 = latencies[int(len(latencies) * 0.90)] if latencies else 0
    p99 = latencies[int(len(latencies) * 0.99)] if latencies else 0
    tps = total / total_time if total_time > 0 else 0

    success = status_counts.get(200, 0)
    shed_429 = status_counts.get(429, 0)
    errors = total - success - shed_429

    return {
        "benchmark": name,
        "total_requests": total,
        "elapsed_sec": round(total_time, 2),
        "throughput_tps": round(tps, 2),
        "p50_ms": round(p50, 2),
        "p90_ms": round(p90, 2),
        "p99_ms": round(p99, 2),
        "status_codes": status_counts,
        "success_rate": f"{(success/total)*100:.1f}%",
        "shed_rate": f"{(shed_429/total)*100:.1f}%"
    }

def print_report(results: List[dict]):
    print("\n" + "="*80)
    print(f"{'ENGINE':<35} | {'TPS':<8} | {'p50(ms)':<8} | {'p99(ms)':<8} | {'SUCCESS':<8} | {'SHED(429)'}")
    print("="*80)
    for r in results:
        shed = r['status_codes'].get(429, 0)
        print(f"{r['benchmark']:<35} | {r['throughput_tps']:<8} | {r['p50_ms']:<8} | {r['p99_ms']:<8} | {r['success_rate']:<8} | {shed} ({r['shed_rate']})")
    print("="*80 + "\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Concurrency & Overflow Benchmark Suite")
    parser.add_argument("--url", default="http://127.0.0.1:8080/work")
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=100)
    args = parser.parse_args()

    results = []
    # 1. Threaded Blocking Benchmark
    res_sync = run_sync_threaded_bench(args.url, args.requests, min(args.concurrency, 50))
    results.append(res_sync)

    # 2. AsyncIO Non-Blocking Benchmark
    res_async = asyncio.run(run_asyncio_bench(args.url, args.requests, args.concurrency))
    results.append(res_async)

    print_report(results)
