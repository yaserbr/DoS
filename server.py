"""
Local educational server demonstrating Rate Limiting.

Two routes:
  - /unprotected : no protection, no artificial delay (real localhost latency).
  - /protected   : protected by Rate Limiting (max 5 requests per 60s per IP).

Run:
    python server.py
"""

import json
import threading
import time
from collections import defaultdict, deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HOST = "127.0.0.1"
PORT = 8000

RATE_LIMIT_MAX_REQUESTS = 5      # max allowed requests
RATE_LIMIT_WINDOW_SECONDS = 60   # within this time window (seconds)

# Global thread-safe request counter
_request_counter = 0
_request_counter_lock = threading.Lock()

# Per-IP request log for /protected: { ip: deque[timestamps] }
_rate_limit_log = defaultdict(deque)
_rate_limit_lock = threading.Lock()


def next_request_number() -> int:
    global _request_counter
    with _request_counter_lock:
        _request_counter += 1
        return _request_counter


def is_rate_limited(ip: str) -> bool:
    """Checks this IP's request count within the time window, and logs the current request if allowed."""
    now = time.time()
    with _rate_limit_lock:
        timestamps = _rate_limit_log[ip]

        # Drop old timestamps that fell outside the time window
        while timestamps and now - timestamps[0] > RATE_LIMIT_WINDOW_SECONDS:
            timestamps.popleft()

        if len(timestamps) >= RATE_LIMIT_MAX_REQUESTS:
            return True

        timestamps.append(now)
        return False


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Disable default logging since we print our own log lines
        pass

    def _send_json(self, status_code: int, payload: dict):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        client_ip = self.client_address[0]
        req_num = next_request_number()

        if self.path == "/unprotected":
            print(f"[UNPROTECTED] Request #{req_num:<5} IP={client_ip:<15} STATUS=200", flush=True)
            self._send_json(200, {
                "route": "unprotected",
                "request_number": req_num,
                "message": "Processed successfully",
            })

        elif self.path == "/protected":
            if is_rate_limited(client_ip):
                print(f"[PROTECTED]   Request #{req_num:<5} IP={client_ip:<15} STATUS=429 (blocked)", flush=True)
                self._send_json(429, {
                    "route": "protected",
                    "request_number": req_num,
                    "message": "Too Many Requests",
                })
            else:
                print(f"[PROTECTED]   Request #{req_num:<5} IP={client_ip:<15} STATUS=200", flush=True)
                self._send_json(200, {
                    "route": "protected",
                    "request_number": req_num,
                    "message": "Processed successfully",
                })

        else:
            self._send_json(404, {"message": "Not found"})


def main():
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"Server running at http://{HOST}:{PORT}")
    print("  - /unprotected : no protection, no artificial delay")
    print(f"  - /protected   : max {RATE_LIMIT_MAX_REQUESTS} requests / {RATE_LIMIT_WINDOW_SECONDS}s per IP")
    print("Press Ctrl+C to stop.\n", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        server.shutdown()


if __name__ == "__main__":
    main()
