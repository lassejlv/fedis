#!/usr/bin/env python3
import os
import socket
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


def worker(port, payload_fn, duration_sec, results, idx):
    s = socket.create_connection(("127.0.0.1", port), timeout=2)
    s.settimeout(2)
    count = 0
    payload = payload_fn(idx)
    start = time.time()
    while time.time() - start < duration_sec:
        s.sendall(payload)
        recv_one(s)
        count += 1
    s.close()
    results[idx] = count


def run_case(port, payload_fn, clients, duration_sec):
    threads = []
    results = [0] * clients
    start = time.time()
    for i in range(clients):
        t = threading.Thread(target=worker, args=(port, payload_fn, duration_sec, results, i))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    elapsed = time.time() - start
    total = sum(results)
    return total / max(elapsed, 0.001)


def main():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    port = 6411
    clients = int(os.environ.get("FEDIS_BENCH_CLIENTS", "16"))
    duration_sec = int(os.environ.get("FEDIS_BENCH_DURATION", "3"))
    mode = os.environ.get("FEDIS_BENCH_MODE", "sharded").strip().lower()

    env = os.environ.copy()
    env["FEDIS_PORT"] = str(port)
    env.setdefault("FEDIS_LOG", "error")

    proc = subprocess.Popen(["cargo", "run", "--quiet"], cwd=root, env=env)
    try:
        s = wait_conn(port)
        s.sendall(encode(["SET", "bench:key:0", "0"]))
        recv_one(s)
        s.close()

        if mode == "hotkey":
            set_payload = lambda _i: encode(["SET", "bench:key:0", "1"])
            get_payload = lambda _i: encode(["GET", "bench:key:0"])
        else:
            set_payload = lambda i: encode(["SET", f"bench:key:{i % max(clients, 1)}", "1"])
            get_payload = lambda i: encode(["GET", f"bench:key:{i % max(clients, 1)}"])

        set_ops = run_case(port, set_payload, clients, duration_sec)
        get_ops = run_case(port, get_payload, clients, duration_sec)

        print(f"clients={clients} duration={duration_sec}s mode={mode}")
        print(f"SET ops/sec: {set_ops:.0f}")
        print(f"GET ops/sec: {get_ops:.0f}")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            proc.kill()


if __name__ == "__main__":
    main()
