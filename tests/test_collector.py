"""
Pytest tests for Python Async Collector
"""
import pytest
import sys
import asyncio
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

from collector_async import (
    generate_reading,
    SensorReading,
    PythonCollector,
    PATIENT_IDS,
    SENSOR_RANGES
)


class TestReadingGeneration:
    """Tests for sensor reading generation"""

    def test_generate_pulse_reading(self):
        """Test generating a pulse reading"""
        reading = generate_reading("pulse")

        assert reading.sensor_type == "pulse"
        assert reading.patient_id in PATIENT_IDS
        assert SENSOR_RANGES["pulse"][0] <= reading.value <= SENSOR_RANGES["pulse"][1]

    def test_generate_temperature_reading(self):
        """Test generating a temperature reading"""
        reading = generate_reading("temperature")

        assert reading.sensor_type == "temperature"
        assert reading.patient_id in PATIENT_IDS
        assert SENSOR_RANGES["temperature"][0] <= reading.value <= SENSOR_RANGES["temperature"][1]

    def test_generate_spo2_reading(self):
        """Test generating an SpO2 reading"""
        reading = generate_reading("spo2")

        assert reading.sensor_type == "spo2"
        assert reading.patient_id in PATIENT_IDS
        assert SENSOR_RANGES["spo2"][0] <= reading.value <= SENSOR_RANGES["spo2"][1]

    def test_generate_blood_pressure_reading(self):
        """Test generating a blood pressure reading"""
        reading = generate_reading("blood_pressure")

        assert reading.sensor_type == "blood_pressure"
        assert reading.patient_id in PATIENT_IDS
        assert reading.systolic is not None
        assert reading.diastolic is not None
        assert SENSOR_RANGES["blood_pressure_sys"][0] <= reading.systolic <= SENSOR_RANGES["blood_pressure_sys"][1]
        assert SENSOR_RANGES["blood_pressure_dia"][0] <= reading.diastolic <= SENSOR_RANGES["blood_pressure_dia"][1]

    def test_reading_to_dict(self):
        """Test converting reading to dictionary"""
        reading = generate_reading("pulse")
        data = reading.to_dict()

        assert isinstance(data, dict)
        assert "patient_id" in data
        assert "sensor_type" in data
        assert "value" in data


class TestCollector:
    """Tests for PythonCollector class"""

    def test_collector_initialization(self):
        """Test collector initialization"""
        collector = PythonCollector(num_readings=100)

        assert collector.num_readings == 100
        assert collector.readings == []

    def test_sync_collection(self):
        """Test synchronous data collection"""
        collector = PythonCollector(num_readings=50)
        readings = collector.collect_sync()

        assert len(readings) == 50
        assert collector.start_time > 0
        assert collector.end_time > 0

    def test_stats_calculation(self):
        """Test statistics calculation"""
        collector = PythonCollector(num_readings=100)
        collector.collect_sync()
        stats = collector.get_stats()

        assert stats["num_readings"] == 100
        assert stats["duration_seconds"] >= 0
        assert stats["readings_per_second"] >= 0
        assert "memory_delta_mb" in stats

    @pytest.mark.asyncio
    async def test_async_collection(self):
        """Test asynchronous data collection"""
        collector = PythonCollector(num_readings=100)
        readings = await collector.collect_async()

        assert len(readings) == 100


class TestSensorRanges:
    """Tests for sensor value ranges"""

    def test_pulse_range(self):
        """Test pulse value range"""
        for _ in range(100):
            reading = generate_reading("pulse")
            assert SENSOR_RANGES["pulse"][0] <= reading.value <= SENSOR_RANGES["pulse"][1]

    def test_temperature_range(self):
        """Test temperature value range"""
        for _ in range(100):
            reading = generate_reading("temperature")
            assert SENSOR_RANGES["temperature"][0] <= reading.value <= SENSOR_RANGES["temperature"][1]

    def test_spo2_range(self):
        """Test SpO2 value range"""
        for _ in range(100):
            reading = generate_reading("spo2")
            assert SENSOR_RANGES["spo2"][0] <= reading.value <= SENSOR_RANGES["spo2"][1]


class TestPatientIDs:
    """Tests for patient ID generation"""

    def test_patient_id_valid(self):
        """Test that generated patient IDs are valid"""
        for _ in range(100):
            reading = generate_reading("pulse")
            assert reading.patient_id in PATIENT_IDS

    def test_patient_id_distribution(self):
        """Test that patient IDs are distributed"""
        readings = [generate_reading("pulse") for _ in range(50)]
        patient_ids = set(r.patient_id for r in readings)

        assert len(patient_ids) > 1, "Should have multiple patients"


class TestAsyncCollectorPerformance:
    """Tests for async collector performance"""

    @pytest.mark.asyncio
    async def test_async_performance(self):
        """Test async collector performance"""
        collector = PythonCollector(num_readings=500)

        start = asyncio.get_event_loop().time()
        await collector.collect_async()
        end = asyncio.get_event_loop().time()

        duration = end - start
        assert duration < 10, "Async collection should be fast"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
