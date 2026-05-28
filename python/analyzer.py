"""
Medical Data Analyzer - Polars, DuckDB, and Visualization
"""
import polars as pl
import duckdb
import json
import time
import sys
from pathlib import Path
from datetime import datetime, timedelta

sys.stdout.reconfigure(encoding='utf-8')

DATA_DIR = Path("D:/projects/lab 14/data")
OUTPUT_DIR = Path("D:/projects/lab 14/data")


def load_jsonl(path: str) -> pl.DataFrame:
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))

    if not records:
        return pl.DataFrame()

    df = pl.DataFrame(records)

    if "timestamp" in df.columns:
        if df["timestamp"].dtype == pl.Float64:
            df = df.with_columns(
                pl.from_epoch("timestamp", time_unit="s").alias("datetime")
            )
        elif df["timestamp"].dtype == pl.Int64:
            df = df.with_columns(
                pl.from_epoch("timestamp", time_unit="s").alias("datetime")
            )

    return df


def load_parquet(path: str) -> pl.DataFrame:
    return pl.read_parquet(path)


def validate_data(df: pl.DataFrame) -> dict:
    validation = {
        "total_records": len(df),
        "issues": []
    }

    if "value" in df.columns and "sensor_type" in df.columns:
        pulse_data = df.filter(pl.col("sensor_type") == "pulse")
        if len(pulse_data) > 0:
            invalid_pulse = pulse_data.filter(
                (pl.col("value") < 30) | (pl.col("value") > 220)
            )
            if len(invalid_pulse) > 0:
                validation["issues"].append(f"Invalid pulse readings: {len(invalid_pulse)}")

        temp_data = df.filter(pl.col("sensor_type") == "temperature")
        if len(temp_data) > 0:
            invalid_temp = temp_data.filter(
                (pl.col("value") < 35) | (pl.col("value") > 42)
            )
            if len(invalid_temp) > 0:
                validation["issues"].append(f"Invalid temperature readings: {len(invalid_temp)}")

        spo2_data = df.filter(pl.col("sensor_type") == "spo2")
        if len(spo2_data) > 0:
            invalid_spo2 = spo2_data.filter(
                (pl.col("value") < 70) | (pl.col("value") > 100)
            )
            if len(invalid_spo2) > 0:
                validation["issues"].append(f"Invalid SpO2 readings: {len(invalid_spo2)}")

    if "systolic" in df.columns:
        invalid_sys = df.filter(
            (pl.col("systolic") < 60) | (pl.col("systolic") > 250)
        )
        if len(invalid_sys) > 0:
            validation["issues"].append(f"Invalid systolic readings: {len(invalid_sys)}")

    if "diastolic" in df.columns:
        invalid_dia = df.filter(
            (pl.col("diastolic") < 40) | (pl.col("diastolic") > 150)
        )
        if len(invalid_dia) > 0:
            validation["issues"].append(f"Invalid diastolic readings: {len(invalid_dia)}")

    return validation


def clean_data(df: pl.DataFrame) -> pl.DataFrame:
    df_clean = df.drop_nulls()

    if "patient_id" in df_clean.columns:
        df_clean = df_clean.unique(subset=["patient_id", "timestamp", "sensor_type"])

    return df_clean


def aggregate_by_sensor(df: pl.DataFrame) -> pl.DataFrame:
    if "value" not in df.columns:
        return pl.DataFrame()

    return df.group_by("sensor_type").agg([
        pl.len().alias("count"),
        pl.col("value").mean().alias("avg"),
        pl.col("value").min().alias("min"),
        pl.col("value").max().alias("max"),
        pl.col("value").std().alias("std"),
    ])


def aggregate_by_patient(df: pl.DataFrame) -> pl.DataFrame:
    if "patient_id" not in df.columns:
        return pl.DataFrame()

    return df.group_by("patient_id").agg([
        pl.len().alias("total_readings"),
        pl.n_unique("sensor_type").alias("sensor_types"),
    ])


def analyze_with_duckdb(parquet_path: str) -> dict:
    conn = duckdb.connect(database=":memory:")

    if not Path(parquet_path).exists():
        return {"error": "Parquet file not found"}

    start_time = time.time()

    result = conn.execute(f"""
        SELECT
            sensor_type,
            COUNT(*) as count,
            AVG(value) as avg_value,
            MIN(value) as min_value,
            MAX(value) as max_value
        FROM '{parquet_path}'
        WHERE sensor_type IS NOT NULL AND value > 0
        GROUP BY sensor_type
        ORDER BY count DESC
    """).fetchdf()

    query_time = time.time() - start_time

    patient_stats = conn.execute(f"""
        SELECT
            patient_id,
            COUNT(*) as readings,
            COUNT(DISTINCT sensor_type) as sensors
        FROM '{parquet_path}'
        GROUP BY patient_id
        ORDER BY readings DESC
    """).fetchdf()

    conn.close()

    return {
        "sensor_stats": result.to_dict(),
        "patient_stats": patient_stats.to_dict(),
        "query_time": round(query_time, 4)
    }


def create_sample_data():
    import random

    patient_ids = ["P001", "P002", "P003", "P004", "P005"]
    sensor_types = ["pulse", "temperature", "spo2", "blood_pressure"]

    records = []
    base_time = datetime.now() - timedelta(hours=1)

    for i in range(5000):
        patient_id = random.choice(patient_ids)
        sensor_type = random.choice(sensor_types)
        timestamp = base_time + timedelta(seconds=i * 0.7)

        record = {
            "patient_id": patient_id,
            "sensor_type": sensor_type,
            "timestamp": timestamp.timestamp(),
            "datetime": timestamp.isoformat(),
        }

        if sensor_type == "blood_pressure":
            record["systolic"] = round(random.uniform(100, 150), 1)
            record["diastolic"] = round(random.uniform(60, 95), 1)
            record["value"] = 0
        elif sensor_type == "pulse":
            record["value"] = round(random.uniform(55, 110), 1)
        elif sensor_type == "temperature":
            record["value"] = round(random.uniform(35.5, 38.0), 2)
        else:
            record["value"] = round(random.uniform(94, 100), 1)

        records.append(record)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_DIR / "medical_data.jsonl", "w", encoding="utf-8") as f:
        for record in records:
            json.dump(record, f)
            f.write("\n")

    return str(OUTPUT_DIR / "medical_data.jsonl")


def main():
    print("=== Medical Data Analyzer ===")
    print()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    jsonl_path = DATA_DIR / "medical_data.jsonl"
    if not jsonl_path.exists():
        print("Generating sample data...")
        jsonl_path = create_sample_data()

    print(f"Loading data from {jsonl_path}...")
    df = load_jsonl(str(jsonl_path))
    print(f"Loaded {len(df)} records")
    print()

    print("=== Data Info ===")
    print(f"Columns: {df.columns}")
    print(f"Shape: {df.shape}")
    print()

    print("=== Validation ===")
    validation = validate_data(df)
    print(f"Total records: {validation['total_records']}")
    for issue in validation["issues"]:
        print(f"  - {issue}")
    print()

    print("=== Cleaning ===")
    df_clean = clean_data(df)
    print(f"Records after cleaning: {len(df_clean)}")
    print()

    print("=== Aggregations ===")
    sensor_stats = aggregate_by_sensor(df_clean)
    print("\nBy Sensor Type:")
    print(sensor_stats)

    patient_stats = aggregate_by_patient(df_clean)
    print("\nBy Patient:")
    print(patient_stats)
    print()

    parquet_path = OUTPUT_DIR / "medical_data.parquet"
    print(f"Saving to Parquet: {parquet_path}")
    df_clean.write_parquet(str(parquet_path))
    print("Done!")
    print()

    print("=== DuckDB Analysis ===")
    duckdb_results = analyze_with_duckdb(str(parquet_path))
    print(f"Query time: {duckdb_results.get('query_time', 'N/A')}s")
    if "sensor_stats" in duckdb_results:
        print("\nSensor Statistics:")
        print(duckdb_results["sensor_stats"])
    print()

    print("=== Summary ===")
    print(f"Input records: {len(df)}")
    print(f"Clean records: {len(df_clean)}")
    print(f"Output file: {parquet_path}")
    print("\nAnalysis complete!")


if __name__ == "__main__":
    main()
