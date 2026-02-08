#!/usr/bin/env python3
import json
import os
import socket
import statistics
import subprocess
import threading
import time


def encode(parts):
    out = f"*{len(parts)}\r\n".encode()
    for p in parts:
        b = p if isinstance(p, bytes) else str(p).encode()
        out += f"${len(b)}\r\n".encode() + b + b"\r\n"
    return out


def recv_one(sock):
    return sock.recv(1024)


def wait_conn(port):
    for _ in range(80):
        try:
            return socket.create_connection(("127.0.0.1", port), timeout=0.2)
        except OSError:
            time.sleep(0.1)
    raise RuntimeError("server did not start")


def run_single(sock, payload, duration_sec):
    start = time.time()
    n = 0
    while time.time() - start < duration_sec:
        sock.sendall(payload)
        recv_one(sock)
        n += 1
    return n / max(time.time() - start, 0.001)


def measure_latency_ms(sock, payload, samples):
    values = []
    for _ in range(samples):
        t0 = time.perf_counter_ns()
        sock.sendall(payload)
        recv_one(sock)
        dt_ms = (time.perf_counter_ns() - t0) / 1_000_000
        values.append(dt_ms)
    values.sort()
    def pct(p):
        idx = min(len(values) - 1, int(len(values) * p))
        return values[idx]
    return {
        "p50_ms": pct(0.50),
        "p95_ms": pct(0.95),
        "p99_ms": pct(0.99),
        "avg_ms": statistics.mean(values),
    }


def worker(port, payload_fn, duration_sec, out, idx):
    s = socket.create_connection(("127.0.0.1", port), timeout=2)
    s.settimeout(2)
    payload = payload_fn(idx)
    count = 0
    start = time.time()
    while time.time() - start < duration_sec:
        s.sendall(payload)
        recv_one(s)
        count += 1
    s.close()
    out[idx] = count


def run_concurrent(port, payload_fn, clients, duration_sec):
    out = [0] * clients
    threads = []
    start = time.time()
    for i in range(clients):
        t = threading.Thread(target=worker, args=(port, payload_fn, duration_sec, out, i))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    total = sum(out)
    return total / max(time.time() - start, 0.001)


def median_runs(fn, runs):
    values = [fn() for _ in range(runs)]
    return statistics.median(values), values


def main():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    port = int(os.environ.get("FEDIS_BENCH_PORT", "6412"))
    runs = int(os.environ.get("FEDIS_BENCH_RUNS", "3"))
    duration = int(os.environ.get("FEDIS_BENCH_DURATION", "2"))
    clients = int(os.environ.get("FEDIS_BENCH_CLIENTS", "16"))

    env = os.environ.copy()
    env["FEDIS_PORT"] = str(port)
    env.setdefault("FEDIS_LOG", "error")
    proc = subprocess.Popen(["cargo", "run", "--quiet"], cwd=root, env=env)

    try:
        sock = wait_conn(port)
        sock.sendall(encode(["SET", "bench:key", "0"]))
        recv_one(sock)

        set_payload = encode(["SET", "bench:key", "123"])
        get_payload = encode(["GET", "bench:key"])
        ping_payload = encode(["PING"])

        run_single(sock, set_payload, 0.5)
        run_single(sock, get_payload, 0.5)

        set_ops, set_samples = median_runs(lambda: run_single(sock, set_payload, duration), runs)
        get_ops, get_samples = median_runs(lambda: run_single(sock, get_payload, duration), runs)
        ping_lat = measure_latency_ms(sock, ping_payload, 200)
        sock.close()

        c_set, c_set_samples = median_runs(
            lambda: run_concurrent(
                port,
                lambda i: encode(["SET", f"bench:key:{i % max(clients, 1)}", "1"]),
                clients,
                duration,
            ),
            runs,
        )
        c_get, c_get_samples = median_runs(
            lambda: run_concurrent(
                port,
                lambda i: encode(["GET", f"bench:key:{i % max(clients, 1)}"]),
                clients,
                duration,
            ),
            runs,
        )

        result = {
            "single": {
                "set_ops_sec_median": set_ops,
                "set_ops_sec_runs": set_samples,
                "get_ops_sec_median": get_ops,
                "get_ops_sec_runs": get_samples,
                "ping_latency_ms": ping_lat,
            },
            "concurrent": {
                "clients": clients,
                "set_ops_sec_median": c_set,
                "set_ops_sec_runs": c_set_samples,
                "get_ops_sec_median": c_get,
                "get_ops_sec_runs": c_get_samples,
            },
        }

        out_path = os.path.join(root, "benchmarks", "latest_results.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2)

        print(json.dumps(result, indent=2))
        print(f"saved: {out_path}")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    main()
