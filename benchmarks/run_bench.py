#!/usr/bin/env python3
import json
import os
import socket
import subprocess
import time


def encode(parts):
    out = f"*{len(parts)}\r\n".encode()
    for p in parts:
        b = p if isinstance(p, bytes) else str(p).encode()
        out += f"${len(b)}\r\n".encode() + b + b"\r\n"
    return out


def recv_one(sock):
    sock.settimeout(2)
    return sock.recv(1024)


def run_loop(sock, payload, duration_sec):
    start = time.time()
    n = 0
    while time.time() - start < duration_sec:
        sock.sendall(payload)
        recv_one(sock)
        n += 1
    elapsed = time.time() - start
    return n / max(elapsed, 0.001)


def wait_conn(port):
    for _ in range(80):
        try:
            return socket.create_connection(("127.0.0.1", port), timeout=0.2)
        except OSError:
            time.sleep(0.1)
    raise RuntimeError("server did not start")


def main():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    with open(os.path.join(root, "benchmarks", "thresholds.json"), "r", encoding="utf-8") as f:
        thresholds = json.load(f)

    port = 6410
    env = os.environ.copy()
    env["FEDIS_PORT"] = str(port)
    env.setdefault("FEDIS_LOG", "error")

    proc = subprocess.Popen(["cargo", "run", "--quiet"], cwd=root, env=env)
    try:
        sock = wait_conn(port)
        sock.sendall(encode(["SET", "bench:key", "0"]))
        recv_one(sock)

        set_ops = run_loop(sock, encode(["SET", "bench:key", "123"]), thresholds["duration_sec"])
        get_ops = run_loop(sock, encode(["GET", "bench:key"]), thresholds["duration_sec"])
        sock.close()

        print(f"SET ops/sec: {set_ops:.0f}")
        print(f"GET ops/sec: {get_ops:.0f}")

        failures = []
        if set_ops < thresholds["set_ops_per_sec_min"]:
            failures.append(
                f"SET ops/sec below threshold: {set_ops:.0f} < {thresholds['set_ops_per_sec_min']}"
            )
        if get_ops < thresholds["get_ops_per_sec_min"]:
            failures.append(
                f"GET ops/sec below threshold: {get_ops:.0f} < {thresholds['get_ops_per_sec_min']}"
            )

        if failures:
            for line in failures:
                print(line)
            raise SystemExit(1)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    main()
