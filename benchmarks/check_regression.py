#!/usr/bin/env python3
import json
import os
import sys


def read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    latest_path = os.path.join(root, "benchmarks", "latest_results.json")
    limits_path = os.path.join(root, "benchmarks", "regression_thresholds.json")

    if not os.path.exists(latest_path):
        print(f"missing benchmark results: {latest_path}")
        print("run: python3 benchmarks/run_suite.py")
        raise SystemExit(1)

    latest = read_json(latest_path)
    limits = read_json(limits_path)

    failures = []

    single_set = latest["single"]["set_ops_sec_median"]
    if single_set < limits["single"]["set_ops_sec_min"]:
        failures.append(
            f"single set ops/sec too low: {single_set:.0f} < {limits['single']['set_ops_sec_min']}"
        )

    single_get = latest["single"]["get_ops_sec_median"]
    if single_get < limits["single"]["get_ops_sec_min"]:
        failures.append(
            f"single get ops/sec too low: {single_get:.0f} < {limits['single']['get_ops_sec_min']}"
        )

    ping_p99 = latest["single"]["ping_latency_ms"]["p99_ms"]
    if ping_p99 > limits["single"]["ping_p99_ms_max"]:
        failures.append(
            f"single ping p99 too high: {ping_p99:.3f}ms > {limits['single']['ping_p99_ms_max']}ms"
        )

    conc_set = latest["concurrent"]["set_ops_sec_median"]
    if conc_set < limits["concurrent"]["set_ops_sec_min"]:
        failures.append(
            f"concurrent set ops/sec too low: {conc_set:.0f} < {limits['concurrent']['set_ops_sec_min']}"
        )

    conc_get = latest["concurrent"]["get_ops_sec_median"]
    if conc_get < limits["concurrent"]["get_ops_sec_min"]:
        failures.append(
            f"concurrent get ops/sec too low: {conc_get:.0f} < {limits['concurrent']['get_ops_sec_min']}"
        )

    if failures:
        print("performance regression check failed")
        for item in failures:
            print(f"- {item}")
        raise SystemExit(1)

    print("performance regression check passed")
    print(f"single set/get: {single_set:.0f}/{single_get:.0f} ops/sec")
    print(f"concurrent set/get: {conc_set:.0f}/{conc_get:.0f} ops/sec")
    print(f"ping p99: {ping_p99:.3f} ms")


if __name__ == "__main__":
    main()
