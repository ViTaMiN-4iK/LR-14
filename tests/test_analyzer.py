"""
Pytest tests for Medical Data Analyzer
"""
import pytest
import sys
import json
import tempfile
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

import polars as pl

VALIDATION_RANGES = {
    "pulse": {"min": 30, "max": 220, "normal_min": 60, "normal_max": 100},
    "temperature": {"min": 35, "max": 42, "normal_min": 36.1, "normal_max": 37.2},
    "spo2": {"min": 70, "max": 100, "normal_min": 95, "normal_max": 100},
    "blood_pressure": {
        "systolic": {"min": 60, "max": 250, "normal_min": 90, "normal_max": 140},
        "diastolic": {"min": 40, "max": 150, "normal_min": 60, "normal_max": 90}
    }
}


def create_sample_data(num_records=100):
    """Create sample medical data for testing"""
    patient_ids = ["P001", "P002", "P003"]
    sensor_types = ["pulse", "temperature", "spo2"]

    records = []
    base_time = datetime.now()

    for i in range(num_records):
        patient_id = patient_ids[i % len(patient_ids)]
        sensor_type = sensor_types[i % len(sensor_types)]

        if sensor_type == "pulse":
            value = 70.0 + (i % 50)
        elif sensor_type == "temperature":
            value = 36.5 + (i % 20) * 0.01
        else:
            value = 97.0 + (i % 3)

        records.append({
            "patient_id": patient_id,
            "sensor_type": sensor_type,
            "value": round(value, 1),
            "timestamp": (base_time - timedelta(seconds=num_records-i)).timestamp()
        })

    return records


class TestValidation:
    """Tests for medical data validation"""

    def test_valid_pulse_range(self):
        """Pulse should be valid within normal range"""
        pulse = 75.0
        assert VALIDATION_RANGES["pulse"]["normal_min"] <= pulse <= VALIDATION_RANGES["pulse"]["normal_max"]

    def test_valid_pulse_extreme_low(self):
        """Pulse at extreme low should still be valid"""
        pulse = 35.0
        assert pulse >= VALIDATION_RANGES["pulse"]["min"]

    def test_invalid_pulse_too_low(self):
        """Pulse below minimum should be invalid"""
        pulse = 25.0
        assert pulse < VALIDATION_RANGES["pulse"]["min"]

    def test_invalid_pulse_too_high(self):
        """Pulse above maximum should be invalid"""
        pulse = 250.0
        assert pulse > VALIDATION_RANGES["pulse"]["max"]

    def test_valid_temperature(self):
        """Temperature within normal range"""
        temp = 36.8
        assert VALIDATION_RANGES["temperature"]["normal_min"] <= temp <= VALIDATION_RANGES["temperature"]["normal_max"]

    def test_invalid_temperature_too_high(self):
        """Temperature above 42 should be invalid"""
        temp = 43.0
        assert temp > VALIDATION_RANGES["temperature"]["max"]

    def test_valid_spo2(self):
        """SpO2 within normal range"""
        spo2 = 98.0
        assert spo2 >= VALIDATION_RANGES["spo2"]["normal_min"]

    def test_invalid_spo2_too_low(self):
        """SpO2 below minimum should be invalid"""
        spo2 = 65.0
        assert spo2 < VALIDATION_RANGES["spo2"]["min"]

    def test_blood_pressure_valid(self):
        """Blood pressure values within range"""
        sys_val = 120.0
        dia_val = 80.0
        assert VALIDATION_RANGES["blood_pressure"]["systolic"]["normal_min"] <= sys_val <= VALIDATION_RANGES["blood_pressure"]["systolic"]["normal_max"]
        assert VALIDATION_RANGES["blood_pressure"]["diastolic"]["normal_min"] <= dia_val <= VALIDATION_RANGES["blood_pressure"]["diastolic"]["normal_max"]


class TestDataFrameOperations:
    """Tests for Polars DataFrame operations"""

    def test_create_dataframe(self):
        """Test creating a Polars DataFrame from sample data"""
        records = create_sample_data(50)
        df = pl.DataFrame(records)

        assert len(df) == 50
        assert "patient_id" in df.columns
        assert "sensor_type" in df.columns
        assert "value" in df.columns

    def test_filter_by_sensor_type(self):
        """Test filtering data by sensor type"""
        records = create_sample_data(100)
        df = pl.DataFrame(records)

        pulse_df = df.filter(pl.col("sensor_type") == "pulse")
        assert len(pulse_df) > 0
        assert (pulse_df["sensor_type"] == "pulse").all()

    def test_group_by_patient(self):
        """Test grouping data by patient"""
        records = create_sample_data(60)
        df = pl.DataFrame(records)

        patient_counts = df.group_by("patient_id").agg([
            pl.len().alias("count")
        ])

        assert len(patient_counts) > 0
        assert "patient_id" in patient_counts.columns
        assert "count" in patient_counts.columns

    def test_aggregate_stats(self):
        """Test aggregation statistics"""
        records = create_sample_data(100)
        df = pl.DataFrame(records)

        pulse_df = df.filter(pl.col("sensor_type") == "pulse")
        stats = pulse_df.select([
            pl.col("value").mean().alias("avg"),
            pl.col("value").min().alias("min"),
            pl.col("value").max().alias("max"),
            pl.len().alias("count")
        ])

        assert stats["count"][0] > 0
        assert stats["min"][0] <= stats["avg"][0] <= stats["max"][0]


class TestDataCleaning:
    """Tests for data cleaning operations"""

    def test_drop_nulls(self):
        """Test removing null values"""
        records = create_sample_data(10)
        records.append({"patient_id": None, "sensor_type": None, "value": None, "timestamp": None})

        df = pl.DataFrame(records)
        df_clean = df.drop_nulls()

        assert len(df_clean) == 10

    def test_remove_duplicates(self):
        """Test removing duplicate records"""
        records = create_sample_data(10)
        records.extend(records[:5])

        df = pl.DataFrame(records)
        df_unique = df.unique()

        assert len(df_unique) == 10


class TestJSONHandling:
    """Tests for JSON file operations"""

    def test_json_roundtrip(self):
        """Test writing and reading JSON"""
        records = create_sample_data(20)

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
            json_path = f.name
            for record in records:
                json.dump(record, f)
                f.write('\n')

        loaded_records = []
        with open(json_path, encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    loaded_records.append(json.loads(line))

        assert len(loaded_records) == 20

        Path(json_path).unlink()


class TestDataConversion:
    """Tests for data type conversions"""

    def test_timestamp_to_datetime(self):
        """Test converting timestamp to datetime"""
        records = create_sample_data(10)
        df = pl.DataFrame(records)

        df = df.with_columns(
            pl.from_epoch("timestamp", time_unit="s").alias("datetime")
        )

        assert "datetime" in df.columns
        assert df["datetime"].dtype == pl.Datetime


class TestSensorRanges:
    """Tests for sensor-specific validations"""

    @pytest.mark.parametrize("sensor_type", ["pulse", "temperature", "spo2"])
    def test_value_within_basic_range(self, sensor_type):
        """Test that generated values are within basic valid range"""
        records = create_sample_data(100)
        df = pl.DataFrame(records)

        sensor_df = df.filter(pl.col("sensor_type") == sensor_type)
        ranges = VALIDATION_RANGES[sensor_type]

        invalid = sensor_df.filter(
            (pl.col("value") < ranges["min"]) | (pl.col("value") > ranges["max"])
        )

        assert len(invalid) == 0, f"Found invalid {sensor_type} values"


class TestPerformance:
    """Tests for performance-related operations"""

    def test_large_dataframe_creation(self):
        """Test creating a large DataFrame"""
        records = create_sample_data(10000)
        df = pl.DataFrame(records)

        assert len(df) == 10000

    def test_aggregation_performance(self):
        """Test that aggregation completes quickly"""
        records = create_sample_data(5000)
        df = pl.DataFrame(records)

        result = df.group_by("sensor_type").agg([
            pl.len().alias("count"),
            pl.col("value").mean().alias("avg")
        ])

        assert len(result) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
