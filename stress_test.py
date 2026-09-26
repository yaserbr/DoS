"""
Concurrent stress test for /unprotected or /protected.

Fires all requests at once using asyncio + aiohttp (no waiting for one
response before sending the next), prints each response the instant it
arrives (unordered, real response time), and immediately cancels every
still-pending request the moment a 429 is received.

Install:
    pip install aiohttp

Run:
    python stress_test.py /unprotected
    python stress_test.py /protected
    python stress_test.py /protected 100      (custom request count, default 30)
"""

import asyncio
import sys
import time

import aiohttp

SERVER_BASE_URL = "http://192.168.1.96:8000"  # <-- ضع هنا IP السيرفر على الشبكة
DEFAULT_REQUEST_COUNT = 300
REQUEST_TIMEOUT_SECONDS = 10
REQUEST_INTERVAL_SECONDS = 0.2 

stop_event = asyncio.Event()
completed_timings = []  # (request_id, elapsed_ms) in completion order


async def fire_request(session: aiohttp.ClientSession, request_id: int, url: str):
    start = time.perf_counter()
    try:
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
        async with session.get(url, timeout=timeout) as response:
            status = response.status
            await response.read()
    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"Request #{request_id:<4} ERROR: {e}", flush=True)
        return

    elapsed_ms = (time.perf_counter() - start) * 1000
    completed_timings.append((request_id, elapsed_ms))
    print(f"Request #{request_id:<4} STATUS={status:<3} TIME={elapsed_ms:7.2f} ms", flush=True)

    if status == 429 and not stop_event.is_set():
        stop_event.set()
        print("-" * 55, flush=True)
        print("Rate limiter triggered: server returned 429 Too Many Requests.", flush=True)
        print("Cancelling all remaining pending requests immediately.", flush=True)


async def cancel_pending_on_stop(tasks):
    await stop_event.wait()
    for task in tasks:
        if not task.done():
            task.cancel()


async def run(path: str, count: int):
    url = SERVER_BASE_URL + path

    print(f"Target         : {url}")
    print(f"Total requests : {count} (sent {REQUEST_INTERVAL_SECONDS * 1000:.0f} ms apart, not waiting for replies)")
    print("-" * 55)

    connector = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        canceller = asyncio.create_task(cancel_pending_on_stop(tasks))

        for i in range(1, count + 1):
            if stop_event.is_set():
                break
            tasks.append(asyncio.create_task(fire_request(session, i, url)))
            await asyncio.sleep(REQUEST_INTERVAL_SECONDS)

        await asyncio.gather(*tasks, return_exceptions=True)
        canceller.cancel()


def print_escalation_summary():
    if not completed_timings:
        print("No completed requests to summarize.")
        return

    first_completed_id, first_completed_ms = completed_timings[0]
    last_completed_id, last_completed_ms = completed_timings[-1]
    fastest_ms = min(t for _, t in completed_timings)
    slowest_ms = max(t for _, t in completed_timings)

    print("\n" + "=" * 55)
    print("Server load / hardware impact summary:")
    print(f"  Completed requests            : {len(completed_timings)}")
    print(f"  1st completed  -> Request #{first_completed_id:<4} : {first_completed_ms:7.2f} ms")
    print(f"  Last completed -> Request #{last_completed_id:<4} : {last_completed_ms:7.2f} ms")
    print(f"  Fastest response time          : {fastest_ms:7.2f} ms")
    print(f"  Slowest response time           : {slowest_ms:7.2f} ms")
    print(f"  Escalation (slowest - fastest) : {slowest_ms - fastest_ms:7.2f} ms")
    print("=" * 55)


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else "/unprotected"
    count = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_REQUEST_COUNT

    if not path.startswith("/"):
        path = "/" + path

    asyncio.run(run(path, count))
    print_escalation_summary()
    print("Stress test finished.")


if __name__ == "__main__":
    main()
