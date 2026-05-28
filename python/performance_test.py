"""
Performance Comparison: Go vs Python Collectors
"""
import time
import psutil
import os
import sys
import json
import subprocess
from pathlib import Path

DATA_DIR = Path("D:/projects/lab 14/data")

def measure_go_performance():
    """Run Go collector and measure performance"""
    print("=== Testing Go Collector ===\n")

    collector_path = Path("D:/projects/lab 14/go/collector/collector.exe")

    if not collector_path.exists():
        print(f"Go collector not found at {collector_path}")
        return None

    process = psutil.Process()
    mem_before = process.memory_info().rss / 1024 / 1024

    start_time = time.time()

    try:
        proc = subprocess.Popen(
            [str(collector_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        time.sleep(5)

        proc.terminate()
        proc.wait(timeout=5)

    except Exception as e:
        print(f"Error running Go collector: {e}")
        return None

    end_time = time.time()
    mem_after = process.memory_info().rss / 1024 / 1024

    duration = end_time - start_time

    return {
        "collector": "Go",
        "duration_seconds": round(duration, 3),
        "memory_delta_mb": round(mem_after - mem_before, 2),
        "note": "Approximate - 5 second test run"
    }

def measure_python_performance():
    """Run Python collector and measure performance"""
    print("=== Testing Python Collector ===\n")

    collector_path = Path("D:/projects/lab 14/python/collector_async.py")

    if not collector_path.exists():
        print(f"Python collector not found at {collector_path}")
        return None

    process = psutil.Process()
    mem_before = process.memory_info().rss / 1024 / 1024

    start_time = time.time()

    try:
        result = subprocess.run(
            [sys.executable, str(collector_path), "async"],
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode != 0:
            print(f"Python collector error: {result.stderr}")

    except Exception as e:
        print(f"Error running Python collector: {e}")
        return None

    end_time = time.time()
    mem_after = process.memory_info().rss / 1024 / 1024

    duration = end_time - start_time

    with open(DATA_DIR / "python_readings.json", "r") as f:
        line_count = sum(1 for _ in f)

    return {
        "collector": "Python (async)",
        "duration_seconds": round(duration, 3),
        "readings_generated": line_count,
        "throughput_per_second": round(line_count / duration, 2) if duration > 0 else 0,
        "memory_delta_mb": round(mem_after - mem_before, 2)
    }

def generate_comparison_report(go_stats, python_stats):
    """Generate comparison report"""
    report = {
        "test_date": time.strftime("%Y-%m-%d %H:%M:%S"),
        "test_parameters": {
            "python_readings": 10000,
            "go_test_duration": "5 seconds"
        },
        "results": {}
    }

    if go_stats:
        report["results"]["go"] = go_stats

    if python_stats:
        report["results"]["python"] = python_stats

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(DATA_DIR / "performance_comparison.json", "w") as f:
        json.dump(report, f, indent=2)

    print("\n=== Performance Comparison Report ===\n")
    print(json.dumps(report, indent=2))

    print("\n=== Summary ===\n")

    if python_stats:
        print(f"Python (async) - {python_stats.get('throughput_per_second', 0):,} readings/sec")
        print(f"  Generated {python_stats.get('readings_generated', 0):,} readings in {python_stats['duration_seconds']}s")

    if go_stats:
        print(f"Go - Test run for {go_stats['duration_seconds']}s")
        print(f"  Note: Go collector runs continuously with ~8 readings/second per sensor")

    print("\nReport saved to data/performance_comparison.json")

    return report

def main():
    print("=" * 60)
    print("Medical Data Collector - Performance Comparison")
    print("=" * 60)
    print()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    python_stats = measure_python_performance()

    go_stats = measure_go_performance()

    report = generate_comparison_report(go_stats, python_stats)

    print("\n" + "=" * 60)
    print("Test Complete")
    print("=" * 60)

if __name__ == "__main__":
    main()
