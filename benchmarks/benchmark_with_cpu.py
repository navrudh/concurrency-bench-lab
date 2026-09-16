"""
Beginner Migration & Scaling Benchmark with Multi-Run Averaging & CPU Tracking
==============================================================================
Empirical benchmarking suite:
1. Multi-run iterations (runs N times, computes mean ± std dev).
2. CPU utilization tracking (user + sys CPU time and % CPU usage of client process).
3. Evaluates Sequential -> ThreadPool (10, 50, 100 workers) -> AsyncIO.
4. Server tuning comparisons under high load.
"""

import argparse
import asyncio
import concurrent.futures
import os
import resource
import statistics
import time
import urllib.request
from typing import List, Tuple, Dict

def get_cpu_times() -> Tuple[float, float]:
    """Returns (user_cpu_time, sys_cpu_time) in seconds for the current process."""
    res = resource.getrusage(resource.RUSAGE_SELF)
    return res.ru_utime, res.ru_stime

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

def single_sync_run(url: str, total_requests: int, max_workers: int) -> Tuple[float, float, List[float], Dict[int, int]]:
    u_start, s_start = get_cpu_times()
    start_total = time.perf_counter()
    latencies = []
    status_counts = {}

    if max_workers == 1:
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
    u_end, s_end = get_cpu_times()
    cpu_time = (u_end - u_start) + (s_end - s_start)
    return total_time, cpu_time, latencies, status_counts

async def single_async_run(host: str, port: int, path: str, total_requests: int, concurrency: int) -> Tuple[float, float, List[float], Dict[int, int]]:
    u_start, s_start = get_cpu_times()
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
    u_end, s_end = get_cpu_times()
    cpu_time = (u_end - u_start) + (s_end - s_start)
    return total_time, cpu_time, latencies, status_counts

def benchmark_sync_multi(name: str, url: str, total_requests: int, max_workers: int, runs: int = 3) -> dict:
    tps_list = []
    cpu_pct_list = []
    p50_list = []
    p99_list = []
    success_rates = []

    for _ in range(runs):
        total_time, cpu_time, latencies, status_counts = single_sync_run(url, total_requests, max_workers)
        latencies.sort()
        p50 = statistics.median(latencies) if latencies else 0
        p99 = latencies[int(len(latencies) * 0.99)] if latencies else 0
        tps = total_requests / total_time if total_time > 0 else 0
        cpu_pct = (cpu_time / total_time) * 100.0 if total_time > 0 else 0
        success = status_counts.get(200, 0)

        tps_list.append(tps)
        cpu_pct_list.append(cpu_pct)
        p50_list.append(p50)
        p99_list.append(p99)
        success_rates.append((success / total_requests) * 100.0)

    return compile_multi_stats(name, total_requests, tps_list, cpu_pct_list, p50_list, p99_list, success_rates)

def benchmark_async_multi(name: str, host: str, port: int, path: str, total_requests: int, concurrency: int, runs: int = 3) -> dict:
    tps_list = []
    cpu_pct_list = []
    p50_list = []
    p99_list = []
    success_rates = []

    for _ in range(runs):
        total_time, cpu_time, latencies, status_counts = asyncio.run(single_async_run(host, port, path, total_requests, concurrency))
        latencies.sort()
        p50 = statistics.median(latencies) if latencies else 0
        p99 = latencies[int(len(latencies) * 0.99)] if latencies else 0
        tps = total_requests / total_time if total_time > 0 else 0
        cpu_pct = (cpu_time / total_time) * 100.0 if total_time > 0 else 0
        success = status_counts.get(200, 0)

        tps_list.append(tps)
        cpu_pct_list.append(cpu_pct)
        p50_list.append(p50)
        p99_list.append(p99)
        success_rates.append((success / total_requests) * 100.0)

    return compile_multi_stats(name, total_requests, tps_list, cpu_pct_list, p50_list, p99_list, success_rates)

def compile_multi_stats(name: str, total: int, tps: List[float], cpu: List[float], p50: List[float], p99: List[float], succ: List[float]) -> dict:
    return {
        "name": name,
        "total": total,
        "runs": len(tps),
        "mean_tps": round(statistics.mean(tps), 1),
        "std_tps": round(statistics.stdev(tps) if len(tps) > 1 else 0.0, 1),
        "mean_cpu": round(statistics.mean(cpu), 1),
        "mean_p50": round(statistics.mean(p50), 1),
        "mean_p99": round(statistics.mean(p99), 1),
        "mean_succ": round(statistics.mean(succ), 1),
    }

def print_multi_table(title: str, rows: List[dict]):
    print(f"\n{title}")
    print(f"{'EXPERIMENT':<40} | {'MEAN TPS ± STD':<18} | {'CPU USAGE':<10} | {'p50(ms)':<8} | {'p99(ms)':<8} | {'SUCCESS'}")
    print("=" * 100)
    for r in rows:
        tps_str = f"{r['mean_tps']} ± {r['std_tps']}"
        cpu_str = f"{r['mean_cpu']}%"
        print(f"{r['name']:<40} | {tps_str:<18} | {cpu_str:<10} | {r['mean_p50']:<8} | {r['mean_p99']:<8} | {r['mean_succ']}%")
    print("=" * 100 + "\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["set1", "set2", "all"], default="all")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--requests", type=int, default=200)
    parser.add_argument("--runs", type=int, default=3)
    args = parser.parse_args()

    url = f"http://{args.host}:{args.port}/work"

    if args.mode in ["set1", "all"]:
        print("\n" + "#"*80)
        print(f"EXPERIMENT SET 1: Sync vs Async Scaling ({args.runs} Multi-Run Benchmark with CPU Tracking)")
        print("#"*80)
        s1 = []

        print(f"-> Testing [1/5] Naive Sequential (1 by 1, {args.runs} runs)...")
        s1.append(benchmark_sync_multi("1. Sequential (1 worker)", url, min(args.requests, 50), max_workers=1, runs=args.runs))

        print(f"-> Testing [2/5] ThreadPool (10 workers, {args.runs} runs)...")
        s1.append(benchmark_sync_multi("2. ThreadPool (10 workers)", url, args.requests, max_workers=10, runs=args.runs))

        print(f"-> Testing [3/5] ThreadPool (50 workers, {args.runs} runs)...")
        s1.append(benchmark_sync_multi("3. ThreadPool (50 workers)", url, args.requests, max_workers=50, runs=args.runs))

        print(f"-> Testing [4/5] ThreadPool (100 workers - thread wall, {args.runs} runs)...")
        s1.append(benchmark_sync_multi("4. ThreadPool (100 workers)", url, args.requests, max_workers=100, runs=args.runs))

        print(f"-> Testing [5/5] Non-Blocking AsyncIO (100 coroutines, {args.runs} runs)...")
        s1.append(benchmark_async_multi("5. AsyncIO (100 coroutines)", args.host, args.port, "/work", args.requests, concurrency=100, runs=args.runs))

        print_multi_table(f"FINAL RESULTS: Experiment Set 1 ({args.runs} Runs Averaged with CPU %)", s1)

    if args.mode in ["set2", "all"]:
        print("\n" + "#"*80)
        print(f"EXPERIMENT SET 2: Extreme Client & Server Tuning ({args.runs} Runs Averaged)")
        print("#"*80)
        s2 = []
        print(f"-> Testing Extreme Client Load (500 coroutines, 1,000 requests, {args.runs} runs)...")
        s2.append(benchmark_async_multi("Extreme AsyncIO (500 coroutines)", args.host, args.port, "/work", total_requests=1000, concurrency=500, runs=args.runs))
        print_multi_table(f"FINAL RESULTS: Experiment Set 2 Under Current Server ({args.runs} Runs)", s2)
