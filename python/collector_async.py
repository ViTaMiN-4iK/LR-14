"""
Python Collector - Async version for performance comparison with Go
"""
import asyncio
import aiohttp
import json
import time
import random
from dataclasses import dataclass, asdict
from typing import List, Optional
import psutil
import os


@dataclass
class SensorReading:
    patient_id: str
    sensor_type: str
    value: float
    systolic: Optional[float] = None
    diastolic: Optional[float] = None
    timestamp: Optional[str] = None

    def to_dict(self):
        return asdict(self)


PATIENT_IDS = ["P001", "P002", "P003", "P004", "P005"]

SENSOR_RANGES = {
    "pulse": (60, 100),
    "temperature": (36.0, 37.0),
    "spo2": (95, 100),
    "blood_pressure_sys": (100, 140),
    "blood_pressure_dia": (60, 90),
}


def generate_reading(sensor_type: str) -> SensorReading:
    patient_id = random.choice(PATIENT_IDS)

    if sensor_type == "blood_pressure":
        sys_val = random.uniform(*SENSOR_RANGES["blood_pressure_sys"])
        dia_val = random.uniform(*SENSOR_RANGES["blood_pressure_dia"])
        return SensorReading(
            patient_id=patient_id,
            sensor_type="blood_pressure",
            value=0,
            systolic=round(sys_val, 1),
            diastolic=round(dia_val, 1),
            timestamp=time.time()
        )
    else:
        range_min, range_max = SENSOR_RANGES.get(sensor_type, (0, 100))
        value = random.uniform(range_min, range_max)
        return SensorReading(
            patient_id=patient_id,
            sensor_type=sensor_type,
            value=round(value, 1),
            timestamp=time.time()
        )


class PythonCollector:
    def __init__(self, num_readings: int = 10000):
        self.num_readings = num_readings
        self.readings: List[SensorReading] = []
        self.start_time = 0
        self.end_time = 0
        self.memory_start = 0
        self.memory_end = 0

    def collect_sync(self) -> List[SensorReading]:
        self.start_time = time.time()
        self.memory_start = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024

        sensor_types = ["pulse", "temperature", "spo2", "blood_pressure"]
        for _ in range(self.num_readings):
            sensor_type = random.choice(sensor_types)
            reading = generate_reading(sensor_type)
            self.readings.append(reading)

        self.end_time = time.time()
        self.memory_end = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024

        return self.readings

    async def collect_async(self) -> List[SensorReading]:
        self.start_time = time.time()
        self.memory_start = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024

        sensor_types = ["pulse", "temperature", "spo2", "blood_pressure"]

        async def generate_batch():
            batch = []
            for _ in range(self.num_readings // 100):
                sensor_type = random.choice(sensor_types)
                reading = generate_reading(sensor_type)
                batch.append(reading)
            return batch

        tasks = [generate_batch() for _ in range(100)]
        results = await asyncio.gather(*tasks)

        for batch in results:
            self.readings.extend(batch)

        self.end_time = time.time()
        self.memory_end = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024

        return self.readings

    def get_stats(self) -> dict:
        duration = self.end_time - self.start_time
        return {
            "num_readings": len(self.readings),
            "duration_seconds": round(duration, 3),
            "readings_per_second": round(len(self.readings) / duration, 2) if duration > 0 else 0,
            "memory_start_mb": round(self.memory_start, 2),
            "memory_end_mb": round(self.memory_end, 2),
            "memory_delta_mb": round(self.memory_end - self.memory_start, 2),
        }


async def main_async():
    print("=== Python Async Collector ===")
    collector = PythonCollector(num_readings=10000)

    print(f"Collecting {collector.num_readings} readings...")
    readings = await collector.collect_async()

    stats = collector.get_stats()
    print(f"\nStatistics:")
    print(f"  Total readings: {stats['num_readings']}")
    print(f"  Duration: {stats['duration_seconds']}s")
    print(f"  Throughput: {stats['readings_per_second']} readings/sec")
    print(f"  Memory start: {stats['memory_start_mb']} MB")
    print(f"  Memory end: {stats['memory_end_mb']} MB")
    print(f"  Memory delta: {stats['memory_delta_mb']} MB")

    with open("D:/projects/lab 14/data/python_readings.json", "w") as f:
        for reading in readings:
            json.dump(reading.to_dict(), f)
            f.write("\n")

    print(f"\nData saved to data/python_readings.json")

    return stats


def main_sync():
    print("=== Python Sync Collector ===")
    collector = PythonCollector(num_readings=10000)

    print(f"Collecting {collector.num_readings} readings...")
    readings = collector.collect_sync()

    stats = collector.get_stats()
    print(f"\nStatistics:")
    print(f"  Total readings: {stats['num_readings']}")
    print(f"  Duration: {stats['duration_seconds']}s")
    print(f"  Throughput: {stats['readings_per_second']} readings/sec")
    print(f"  Memory start: {stats['memory_start_mb']} MB")
    print(f"  Memory end: {stats['memory_end_mb']} MB")
    print(f"  Memory delta: {stats['memory_delta_mb']} MB")

    with open("D:/projects/lab 14/data/python_readings.json", "w") as f:
        for reading in readings:
            json.dump(reading.to_dict(), f)
            f.write("\n")

    print(f"\nData saved to data/python_readings.json")

    return stats


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "async":
        asyncio.run(main_async())
    else:
        main_sync()
