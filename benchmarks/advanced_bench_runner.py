import argparse
import asyncio
import concurrent.futures
import json
import statistics
import time
import http.client
import threading
import random
from urllib.parse import urlparse
from typing import List, Tuple

thread_local = threading.local()

def get_http_connection(host, port):
    if not hasattr(thread_local, "conn"):
        thread_local.conn = http.client.HTTPConnection(host, port, timeout=5.0)
    return thread_local.conn

def make_sync_keepalive_request(host: str, port: int, path: str) -> Tuple[int, float]:
    start = time.perf_counter()
    try:
        conn = get_http_connection(host, port)
        conn.request("GET", path, headers={"Connection": "keep-alive"})
        resp = conn.getresponse()
        resp.read() # Consume body
        elapsed = (time.perf_counter() - start) * 1000.0
        return resp.status, elapsed
    except Exception as e:
        if hasattr(thread_local, "conn"):
            thread_local.conn.close()
            del thread_local.conn
        elapsed = (time.perf_counter() - start) * 1000.0
        return 0, elapsed

def run_sync_keepalive_bench(url: str, total_requests: int, concurrency: int) -> dict:
    parsed = urlparse(url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 80
    path = parsed.path or "/"
    if parsed.query: path += f"?{parsed.query}"
    
    print(f"\n[1] Running Threaded Keep-Alive Benchmark (threads={concurrency}, total={total_requests})...")
    start_total = time.perf_counter()
    latencies = []
    status_counts = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(make_sync_keepalive_request, host, port, path) for _ in range(total_requests)]
        for f in concurrent.futures.as_completed(futures):
            code, elapsed = f.result()
            latencies.append(elapsed)
            status_counts[code] = status_counts.get(code, 0) + 1

    total_time = time.perf_counter() - start_total
    return compile_stats("Threaded Keep-Alive", total_requests, total_time, latencies, status_counts)

class AsyncConnectionPool:
    def __init__(self, host, port, size):
        self.host = host
        self.port = port
        self.pool = asyncio.Queue(maxsize=size)
        self.size = size
        
    async def get_connection(self):
        if self.pool.empty() and self.size > 0:
            self.size -= 1
            reader, writer = await asyncio.open_connection(self.host, self.port)
            return reader, writer
        return await self.pool.get()
        
    async def release_connection(self, conn):
        await self.pool.put(conn)

async def make_async_keepalive_request(pool: AsyncConnectionPool, host: str, port: int, path: str) -> Tuple[int, float]:
    start = time.perf_counter()
    reader, writer = await pool.get_connection()
    try:
        req = f"GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: keep-alive\r\nUser-Agent: BenchAsync/1.0\r\n\r\n"
        writer.write(req.encode("latin1"))
        await writer.drain()

        # Read status line
        line = await reader.readline()
        if not line: raise ConnectionError("Connection closed")
        parts = line.decode("latin1", errors="replace").split(" ")
        status = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
        
        # Read headers until empty line
        content_length = 0
        while True:
            header = await reader.readline()
            if header in (b"\r\n", b"\n", b""):
                break
            h_str = header.decode("latin1").lower()
            if h_str.startswith("content-length:"):
                content_length = int(h_str.split(":")[1].strip())
        
        # Read body
        if content_length > 0:
            await reader.readexactly(content_length)

        elapsed = (time.perf_counter() - start) * 1000.0
        await pool.release_connection((reader, writer))
        return status, elapsed
    except Exception:
        writer.close()
        try: await writer.wait_closed()
        except: pass
        pool.size += 1
        elapsed = (time.perf_counter() - start) * 1000.0
        return 0, elapsed

async def run_asyncio_keepalive_bench(url: str, total_requests: int, concurrency: int) -> dict:
    parsed = urlparse(url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 80
    path = parsed.path or "/"
    if parsed.query: path += f"?{parsed.query}"

    print(f"\n[2] Running AsyncIO Keep-Alive Benchmark (concurrency={concurrency}, total={total_requests})...")
    start_total = time.perf_counter()
    latencies = []
    status_counts = {}

    pool = AsyncConnectionPool(host, port, concurrency)
    
    async def worker():
        return await make_async_keepalive_request(pool, host, port, path)

    tasks = [asyncio.create_task(worker()) for _ in range(total_requests)]
    results = await asyncio.gather(*tasks)

    for code, elapsed in results:
        latencies.append(elapsed)
        status_counts[code] = status_counts.get(code, 0) + 1

    total_time = time.perf_counter() - start_total
    return compile_stats("AsyncIO Keep-Alive", total_requests, total_time, latencies, status_counts)

async def run_adaptive_concurrency_bench(url: str, total_requests: int, max_concurrency: int) -> dict:
    parsed = urlparse(url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 80
    path = parsed.path or "/"
    if parsed.query: path += f"?{parsed.query}"

    print(f"\n[3] Running Adaptive Concurrency Limit Benchmark (max_concurrency={max_concurrency}, total={total_requests})...")
    start_total = time.perf_counter()
    latencies = []
    status_counts = {}

    # Netflix / TCP Vegas style AIMD
    current_limit = 10
    target_latency = 100.0 # ms
    
    pool = AsyncConnectionPool(host, port, max_concurrency)
    
    # We use a custom semaphore that we can resize, or just manually control in-flight
    in_flight = 0
    cond = asyncio.Condition()
    completed = 0
    
    async def worker():
        nonlocal in_flight, current_limit, completed
        async with cond:
            while in_flight >= int(current_limit):
                await cond.wait()
            in_flight += 1
            
        code, elapsed = await make_async_keepalive_request(pool, host, port, path)
        
        async with cond:
            in_flight -= 1
            # Adaptive logic
            if code == 200 and elapsed < target_latency:
                current_limit = min(current_limit + 0.5, max_concurrency)
            else:
                current_limit = max(current_limit * 0.8, 1.0)
                
            latencies.append(elapsed)
            status_counts[code] = status_counts.get(code, 0) + 1
            completed += 1
            cond.notify_all()

    tasks = [asyncio.create_task(worker()) for _ in range(total_requests)]
    await asyncio.gather(*tasks)

    total_time = time.perf_counter() - start_total
    return compile_stats("AsyncIO Adaptive Concurrency (Vegas)", total_requests, total_time, latencies, status_counts)

async def run_retry_storm_bench(url: str, total_requests: int, concurrency: int) -> dict:
    parsed = urlparse(url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 80
    path = parsed.path or "/"
    if parsed.query: path += f"?{parsed.query}"

    print(f"\n[4] Running Retry Storm & Exp Backoff Benchmark (concurrency={concurrency}, total={total_requests})...")
    start_total = time.perf_counter()
    latencies = []
    status_counts = {}

    pool = AsyncConnectionPool(host, port, concurrency)
    sem = asyncio.Semaphore(concurrency)
    
    async def worker():
        retries = 0
        max_retries = 5
        base_delay = 0.05
        
        while retries <= max_retries:
            async with sem:
                code, elapsed = await make_async_keepalive_request(pool, host, port, path)
            
            if code == 429:
                retries += 1
                if retries <= max_retries:
                    delay = base_delay * (2 ** retries)
                    jitter = random.uniform(0, delay * 0.2)
                    await asyncio.sleep(delay + jitter)
                else:
                    return code, elapsed
            else:
                return code, elapsed

    tasks = [asyncio.create_task(worker()) for _ in range(total_requests)]
    results = await asyncio.gather(*tasks)

    for code, elapsed in results:
        latencies.append(elapsed)
        status_counts[code] = status_counts.get(code, 0) + 1

    total_time = time.perf_counter() - start_total
    return compile_stats("AsyncIO Exp Backoff + Jitter", total_requests, total_time, latencies, status_counts)


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
    print("\n" + "="*95)
    print(f"{'ENGINE':<35} | {'TPS':<8} | {'p50(ms)':<8} | {'p99(ms)':<8} | {'SUCCESS':<8} | {'SHED(429)'}")
    print("="*95)
    for r in results:
        shed = r['status_codes'].get(429, 0)
        print(f"{r['benchmark']:<35} | {r['throughput_tps']:<8} | {r['p50_ms']:<8} | {r['p99_ms']:<8} | {r['success_rate']:<8} | {shed} ({r['shed_rate']})")
    print("="*95 + "\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8080/work")
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=100)
    args = parser.parse_args()

    results = []
    
    # 1. Threaded Keep-Alive
    res_sync = run_sync_keepalive_bench(args.url, args.requests, min(args.concurrency, 50))
    results.append(res_sync)

    # 2. AsyncIO Keep-Alive
    res_async = asyncio.run(run_asyncio_keepalive_bench(args.url, args.requests, args.concurrency))
    results.append(res_async)

    # 3. AsyncIO Adaptive Concurrency
    res_adaptive = asyncio.run(run_adaptive_concurrency_bench(args.url, args.requests, args.concurrency))
    results.append(res_adaptive)

    # 4. AsyncIO Retry Storm
    res_retry = asyncio.run(run_retry_storm_bench(args.url, args.requests, args.concurrency))
    results.append(res_retry)

    print_report(results)
