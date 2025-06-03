"""
Pure Python asynchronous high-concurrency mock HTTP server.
Uses asyncio + aiohttp / standard asyncio protocol for buffer queuing and request shedding.
Provides zero-dependency fallback to run directly on Python 3 without compiling Rust.
"""

import argparse
import asyncio
import json
import time
from typing import Optional

class BufferMockServer:
    def __init__(self, host: str = "0.0.0.0", port: int = 8080, concurrency: int = 100, queue_cap: int = 500, delay_ms: int = 50):
        self.host = host
        self.port = port
        self.concurrency = concurrency
        self.queue_cap = queue_cap
        self.delay_ms = delay_ms

        self.active_sem = asyncio.Semaphore(concurrency)
        self.queue_sem = asyncio.Semaphore(queue_cap)

        self.total_received = 0
        self.total_processed = 0
        self.total_shed = 0
        self.current_queued = 0
        self.current_active = 0

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break

                req_line = line.decode("utf-8", errors="replace").strip()
                if not req_line:
                    break
                parts = req_line.split(" ")
                method = parts[0] if len(parts) > 0 else "GET"
                path = parts[1] if len(parts) > 1 else "/"

                # Drain headers and check keep-alive
                keep_alive = False
                while True:
                    h = await reader.readline()
                    if h in (b"\r\n", b"\n", b""):
                        break
                    h_str = h.decode("latin1", errors="ignore").lower()
                    if h_str.startswith("connection:") and "keep-alive" in h_str:
                        keep_alive = True

                conn_header = "keep-alive" if keep_alive else "close"

                if path.startswith("/health"):
                    body = b"OK"
                    resp = f"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {len(body)}\r\nConnection: {conn_header}\r\n\r\n".encode("latin1") + body
                    writer.write(resp)
                    await writer.drain()
                    if not keep_alive: break
                    continue

                if path.startswith("/metrics"):
                    metrics = {
                        "received": self.total_received,
                        "processed": self.total_processed,
                        "shed": self.total_shed,
                        "current_queued": self.current_queued,
                        "current_active": self.current_active
                    }
                    body = json.dumps(metrics).encode("utf-8")
                    resp = f"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\nConnection: {conn_header}\r\n\r\n".encode("latin1") + body
                    writer.write(resp)
                    await writer.drain()
                    if not keep_alive: break
                    continue

                if path.startswith("/work"):
                    self.total_received += 1

                    # 1. Try acquire queue permit (Admission control)
                    if self.queue_sem.locked() and self.queue_sem._value <= 0:
                        self.total_shed += 1
                        body = b'{"error": "Buffer Full: Request Shed"}'
                        resp = f"HTTP/1.1 429 Too Many Requests\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\nConnection: {conn_header}\r\n\r\n".encode("latin1") + body
                        writer.write(resp)
                        await writer.drain()
                        if not keep_alive: break
                        continue

                    await self.queue_sem.acquire()
                    self.current_queued += 1

                    # 2. Wait for worker slot
                    await self.active_sem.acquire()
                    self.queue_sem.release()
                    self.current_queued -= 1
                    self.current_active += 1

                    # 3. Process simulated latency
                    await asyncio.sleep(self.delay_ms / 1000.0)

                    self.current_active -= 1
                    self.total_processed += 1
                    self.active_sem.release()

                    body = json.dumps({"status": "success"}).encode("utf-8")
                    resp = f"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\nConnection: {conn_header}\r\n\r\n".encode("latin1") + body
                    writer.write(resp)
                    await writer.drain()
                    if not keep_alive: break
                    continue

                # Default 404
                body = b"Not Found"
                resp = f"HTTP/1.1 404 Not Found\r\nContent-Length: {len(body)}\r\nConnection: {conn_header}\r\n\r\n".encode("latin1") + body
                writer.write(resp)
                await writer.drain()
                if not keep_alive: break

        except Exception:
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def run(self):
        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        print("===============================================================")
        print(f"⚡ Python High-Throughput Mock Queue Server")
        print(f"Listening on:           {self.host}:{self.port}")
        print(f"Active Worker Limit:    {self.concurrency}")
        print(f"Queue/Buffer Capacity:  {self.queue_cap}")
        print(f"Simulated Delay:        {self.delay_ms} ms")
        print("===============================================================")
        async with server:
            await server.serve_forever()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--queue-cap", type=int, default=500)
    parser.add_argument("--delay-ms", type=int, default=50)
    args = parser.parse_args()

    s = BufferMockServer(args.host, args.port, args.concurrency, args.queue_cap, args.delay_ms)
    asyncio.run(s.run())
