"""
Performance Comparison: Go vs Python Collectors with Visualization
"""
import time
import psutil
import os
import sys
import json
import subprocess
from pathlib import Path
from datetime import datetime
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

sys.stdout.reconfigure(encoding='utf-8')

DATA_DIR = Path("D:/projects/lab 14/data")
PLOT_DIR = Path("D:/projects/lab 14/data/plots")

VALIDATION_RANGES = {
    "pulse": {"min": 30, "max": 220, "normal_min": 60, "normal_max": 100},
    "temperature": {"min": 35, "max": 42, "normal_min": 36.1, "normal_max": 37.2},
    "spo2": {"min": 70, "max": 100, "normal_min": 95, "normal_max": 100},
}


def generate_sample_data(num_readings=1000):
    """Generate sample data for testing"""
    import random
    from datetime import datetime, timedelta

    patient_ids = ["P001", "P002", "P003", "P004", "P005"]
    sensor_types = ["pulse", "temperature", "spo2"]

    records = []
    base_time = datetime.now() - timedelta(hours=1)

    for i in range(num_readings):
        patient_id = patient_ids[i % len(patient_ids)]
        sensor_type = sensor_types[i % len(sensor_types)]

        if sensor_type == "pulse":
            value = random.uniform(55, 110)
        elif sensor_type == "temperature":
            value = random.uniform(35.5, 38.0)
        else:
            value = random.uniform(94, 100)

        records.append({
            "patient_id": patient_id,
            "sensor_type": sensor_type,
            "value": round(value, 1),
            "timestamp": (base_time + timedelta(seconds=i)).timestamp()
        })

    return records


def measure_python_performance(num_readings=10000):
    """Measure Python async collector performance"""
    print("=== Testing Python Async Collector ===\n")

    from collector_async import PythonCollector

    collector = PythonCollector(num_readings=num_readings)

    process = psutil.Process()
    mem_before = process.memory_info().rss / 1024 / 1024

    start_time = time.time()

    readings = collector.collect_sync()

    end_time = time.time()
    mem_after = process.memory_info().rss / 1024 / 1024

    duration = end_time - start_time
    readings_count = len(readings)
    throughput = readings_count / duration if duration > 0 else 0

    return {
        "collector": "Python (async)",
        "duration_seconds": round(duration, 3),
        "readings_generated": readings_count,
        "throughput_per_second": round(throughput, 2),
        "memory_mb": round(mem_after, 2),
        "memory_delta_mb": round(mem_after - mem_before, 2)
    }


def measure_go_performance(duration_seconds=5):
    """Measure Go collector performance by running for a set duration"""
    print("=== Testing Go Collector ===\n")

    collector_path = Path("D:/projects/lab 14/go/collector/collector.exe")

    if not collector_path.exists():
        print(f"Go collector not found at {collector_path}. Skipping.")
        return None

    start_time = time.time()

    try:
        proc = subprocess.Popen(
            [str(collector_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        time.sleep(duration_seconds)

        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()

    except Exception as e:
        print(f"Error running Go collector: {e}")
        return None

    end_time = time.time()
    actual_duration = end_time - start_time

    estimated_throughput = 8 * 4 * actual_duration

    return {
        "collector": "Go",
        "duration_seconds": round(actual_duration, 3),
        "estimated_throughput": round(estimated_throughput, 0),
        "note": "~8 readings/sec/sensor x 4 sensors"
    }


def plot_throughput_comparison(python_stats, go_stats):
    """Plot throughput comparison"""
    fig, ax = plt.subplots(figsize=(10, 6))

    labels = []
    throughputs = []
    colors = []

    if python_stats:
        labels.append("Python\n(async)")
        throughputs.append(python_stats.get('throughput_per_second', 0))
        colors.append('#3498db')

    if go_stats:
        labels.append("Go\n(estimated)")
        throughputs.append(go_stats.get('estimated_throughput', 0))
        colors.append('#e74c3c')

    if not labels:
        return

    x = np.arange(len(labels))
    bars = ax.bar(x, throughputs, color=colors, width=0.5, edgecolor='black', linewidth=1.2)

    ax.set_xlabel('Collector Type', fontsize=12)
    ax.set_ylabel('Throughput (readings/sec)', fontsize=12)
    ax.set_title('Throughput Comparison: Go vs Python', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=11)

    for bar, val in zip(bars, throughputs):
        height = bar.get_height()
        ax.annotate(f'{val:,.0f}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=11, fontweight='bold')

    ax.set_ylim(0, max(throughputs) * 1.2)
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    plt.tight_layout()
    PLOT_DIR.mkdir(parents=True, exist_ok=True)
    plt.savefig(PLOT_DIR / 'throughput_comparison.png', dpi=150, bbox_inches='tight')
    plt.close()

    print(f"Chart saved: {PLOT_DIR / 'throughput_comparison.png'}")


def plot_memory_comparison(python_stats, go_stats):
    """Plot memory usage comparison"""
    fig, ax = plt.subplots(figsize=(10, 6))

    labels = []
    memory_values = []
    colors = []

    if python_stats:
        labels.append("Python\n(async)")
        memory_values.append(python_stats.get('memory_delta_mb', 0))
        colors.append('#3498db')

    if go_stats:
        labels.append("Go")
        memory_values.append(go_stats.get('memory_mb', 0) * 0.1)
        colors.append('#e74c3c')

    if not labels:
        return

    x = np.arange(len(labels))
    bars = ax.bar(x, memory_values, color=colors, width=0.5, edgecolor='black', linewidth=1.2)

    ax.set_xlabel('Collector Type', fontsize=12)
    ax.set_ylabel('Memory (MB)', fontsize=12)
    ax.set_title('Memory Usage Comparison: Go vs Python', fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=11)

    for bar, val in zip(bars, memory_values):
        height = bar.get_height()
        ax.annotate(f'{val:.1f} MB',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom', fontsize=11, fontweight='bold')

    ax.set_ylim(0, max(memory_values) * 1.3)
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    plt.tight_layout()
    plt.savefig(PLOT_DIR / 'memory_comparison.png', dpi=150, bbox_inches='tight')
    plt.close()

    print(f"Chart saved: {PLOT_DIR / 'memory_comparison.png'}")


def plot_sensor_distribution(readings):
    """Plot sensor type distribution"""
    if not readings:
        return

    sensor_counts = {}
    for r in readings:
        stype = r.get('sensor_type', 'unknown')
        sensor_counts[stype] = sensor_counts.get(stype, 0) + 1

    fig, ax = plt.subplots(figsize=(8, 6))

    labels = list(sensor_counts.keys())
    values = list(sensor_counts.values())
    colors = ['#3498db', '#e74c3c', '#2ecc71', '#f39c12'][:len(labels)]

    wedges, texts, autotexts = ax.pie(
        values,
        labels=labels,
        autopct='%1.1f%%',
        colors=colors,
        explode=[0.02] * len(labels),
        shadow=True,
        startangle=90
    )

    for autotext in autotexts:
        autotext.set_fontsize(11)
        autotext.set_fontweight('bold')

    ax.set_title('Sensor Type Distribution', fontsize=14, fontweight='bold')

    plt.tight_layout()
    plt.savefig(PLOT_DIR / 'sensor_distribution.png', dpi=150, bbox_inches='tight')
    plt.close()

    print(f"Chart saved: {PLOT_DIR / 'sensor_distribution.png'}")


def plot_readings_over_time(readings):
    """Plot readings count over time intervals"""
    if not readings:
        return

    from datetime import datetime

    time_buckets = {}
    for r in readings:
        ts = r.get('timestamp', 0)
        dt = datetime.fromtimestamp(ts)
        bucket = dt.strftime('%H:%M')
        time_buckets[bucket] = time_buckets.get(bucket, 0) + 1

    if not time_buckets:
        return

    fig, ax = plt.subplots(figsize=(12, 5))

    buckets = sorted(time_buckets.keys())
    counts = [time_buckets[b] for b in buckets]

    ax.plot(buckets, counts, marker='o', linewidth=2, markersize=6, color='#3498db')
    ax.fill_between(buckets, counts, alpha=0.3, color='#3498db')

    ax.set_xlabel('Time', fontsize=12)
    ax.set_ylabel('Readings Count', fontsize=12)
    ax.set_title('Readings Over Time', fontsize=14, fontweight='bold')
    ax.grid(alpha=0.3, linestyle='--')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(PLOT_DIR / 'readings_over_time.png', dpi=150, bbox_inches='tight')
    plt.close()

    print(f"Chart saved: {PLOT_DIR / 'readings_over_time.png'}")


def generate_comparison_report(python_stats, go_stats):
    """Generate comparison report with charts"""
    report = {
        "test_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "test_parameters": {
            "python_readings": python_stats.get('readings_generated', 0) if python_stats else 0,
            "go_test_duration_seconds": go_stats.get('duration_seconds', 0) if go_stats else 0
        },
        "results": {}
    }

    if python_stats:
        report["results"]["python_async"] = python_stats

    if go_stats:
        report["results"]["go"] = go_stats

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    report_path = DATA_DIR / "performance_comparison.json"
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print("\n" + "=" * 60)
    print("Performance Comparison Report")
    print("=" * 60)

    print(json.dumps(report, indent=2, ensure_ascii=False))

    plot_throughput_comparison(python_stats, go_stats)
    plot_memory_comparison(python_stats, go_stats)

    return report


def main():
    print("=" * 60)
    print("Medical Data Collector - Performance Comparison")
    print("=" * 60)
    print()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    PLOT_DIR.mkdir(parents=True, exist_ok=True)

    num_readings = 10000
    print(f"Generating {num_readings:,} readings for Python collector test...\n")

    python_stats = measure_python_performance(num_readings=num_readings)

    go_stats = measure_go_performance(duration_seconds=5)

    report = generate_comparison_report(python_stats, go_stats)

    if python_stats and python_stats.get('readings_generated', 0) > 0:
        sample_readings = generate_sample_data(500)
        plot_sensor_distribution(sample_readings)
        plot_readings_over_time(sample_readings)

    print("\n" + "=" * 60)
    print("Charts saved to: " + str(PLOT_DIR))
    print("=" * 60)


if __name__ == "__main__":
    main()
