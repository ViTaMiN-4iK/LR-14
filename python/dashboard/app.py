"""
Streamlit Dashboard - Medical Data Visualization
Real-time monitoring of health sensors
"""
import streamlit as st
import polars as pl
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import time
import random
from datetime import datetime, timedelta
from pathlib import Path
import os

st.set_page_config(
    page_title="Medical Data Dashboard",
    page_icon="🏥",
    layout="wide"
)

DATA_DIR = Path("D:/projects/lab 14/data")

VALIDATION_RANGES = {
    "pulse": {"min": 30, "max": 220, "normal_min": 60, "normal_max": 100},
    "temperature": {"min": 35, "max": 42, "normal_min": 36.1, "normal_max": 37.2},
    "spo2": {"min": 70, "max": 100, "normal_min": 95, "normal_max": 100},
    "blood_pressure": {
        "systolic": {"min": 60, "max": 250, "normal_min": 90, "normal_max": 140},
        "diastolic": {"min": 40, "max": 150, "normal_min": 60, "normal_max": 90}
    }
}

def load_data():
    """Load data from Parquet file or generate sample data"""
    parquet_path = DATA_DIR / "medical_data.parquet"
    jsonl_path = DATA_DIR / "medical_data.jsonl"

    if parquet_path.exists():
        df = pl.read_parquet(str(parquet_path))
    elif jsonl_path.exists():
        df = pl.read_ndjson(str(jsonl_path))
    else:
        df = generate_sample_data()

    return df

def generate_sample_data():
    """Generate sample medical data for demo"""
    patient_ids = ["P001", "P002", "P003", "P004", "P005"]
    sensor_types = ["pulse", "temperature", "spo2"]

    records = []
    base_time = datetime.now() - timedelta(hours=2)

    for i in range(1000):
        patient_id = random.choice(patient_ids)
        sensor_type = random.choice(sensor_types)
        timestamp = base_time + timedelta(minutes=i * 0.1)

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
            "timestamp": timestamp.timestamp(),
            "datetime": timestamp
        })

    df = pl.DataFrame(records)
    return df

def get_alerts(df):
    """Check for abnormal readings"""
    alerts = []

    for sensor, ranges in VALIDATION_RANGES.items():
        if sensor == "blood_pressure":
            continue

        sensor_data = df.filter(pl.col("sensor_type") == sensor)
        if len(sensor_data) == 0:
            continue

        low = sensor_data.filter(pl.col("value") < ranges["normal_min"])
        high = sensor_data.filter(pl.col("value") > ranges["normal_max"])

        if len(low) > 0:
            alerts.append(f"Low {sensor}: {len(low)} readings below normal")
        if len(high) > 0:
            alerts.append(f"High {sensor}: {len(high)} readings above normal")

    return alerts

def plot_time_series(df, sensor_type, title):
    """Create time series plot for a sensor"""
    sensor_data = df.filter(pl.col("sensor_type") == sensor_type)

    if len(sensor_data) == 0:
        return None

    ranges = VALIDATION_RANGES.get(sensor_type, {})

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=sensor_data["datetime"],
        y=sensor_data["value"],
        mode="lines",
        name=sensor_type,
        line=dict(color="blue", width=2)
    ))

    if "normal_min" in ranges:
        fig.add_hline(
            y=ranges["normal_min"],
            line_dash="dash",
            line_color="green",
            annotation_text=f"Normal Min ({ranges['normal_min']})"
        )
        fig.add_hline(
            y=ranges["normal_max"],
            line_dash="dash",
            line_color="orange",
            annotation_text=f"Normal Max ({ranges['normal_max']})"
        )

    fig.update_layout(
        title=title,
        xaxis_title="Time",
        yaxis_title="Value",
        height=300,
        margin=dict(l=20, r=20, t=40, b=20)
    )

    return fig

def plot_distribution(df, sensor_type):
    """Create histogram for sensor values"""
    sensor_data = df.filter(pl.col("sensor_type") == sensor_type)

    if len(sensor_data) == 0:
        return None

    fig = px.histogram(
        sensor_data.to_pandas(),
        x="value",
        nbins=30,
        title=f"{sensor_type.title()} Distribution",
        labels={"value": "Value", "count": "Count"}
    )

    fig.update_layout(height=250, showlegend=False)

    return fig

def plot_patient_summary(df):
    """Create patient summary bar chart"""
    patient_counts = df.group_by("patient_id").agg([
        pl.len().alias("readings"),
        pl.n_unique("sensor_type").alias("sensors")
    ])

    if len(patient_counts) == 0:
        return None

    fig = px.bar(
        patient_counts.to_pandas(),
        x="patient_id",
        y="readings",
        color="sensors",
        title="Readings per Patient",
        labels={"patient_id": "Patient", "readings": "Total Readings", "sensors": "Sensors"}
    )

    return fig

def main():
    st.title("🏥 Medical Data Dashboard")
    st.markdown("---")

    df = load_data()

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_readings = len(df)
        st.metric("Total Readings", total_readings)

    with col2:
        num_patients = df["patient_id"].n_unique() if "patient_id" in df.columns else 0
        st.metric("Patients", num_patients)

    with col3:
        num_sensors = df["sensor_type"].n_unique() if "sensor_type" in df.columns else 0
        st.metric("Sensor Types", num_sensors)

    with col4:
        if "datetime" in df.columns:
            latest = df["datetime"].max()
            if latest:
                time_diff = datetime.now() - latest
                st.metric("Last Update", f"{time_diff.seconds}s ago")
            else:
                st.metric("Last Update", "N/A")
        else:
            st.metric("Last Update", "N/A")

    st.markdown("---")

    alerts = get_alerts(df)
    if alerts:
        st.warning("⚠️ Alerts Detected:")
        for alert in alerts:
            st.write(f"- {alert}")
        st.markdown("---")

    st.subheader("Sensor Readings Over Time")

    tab1, tab2, tab3 = st.tabs(["Pulse", "Temperature", "SpO2"])

    with tab1:
        fig = plot_time_series(df, "pulse", "Pulse Rate (bpm)")
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No pulse data available")

    with tab2:
        fig = plot_time_series(df, "temperature", "Body Temperature (°C)")
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No temperature data available")

    with tab3:
        fig = plot_time_series(df, "spo2", "Blood Oxygen Saturation (%)")
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No SpO2 data available")

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Value Distributions")
        for sensor in ["pulse", "temperature", "spo2"]:
            fig = plot_distribution(df, sensor)
            if fig:
                st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Patient Overview")
        fig = plot_patient_summary(df)
        if fig:
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    st.subheader("Recent Readings")

    if "datetime" in df.columns:
        recent = df.sort("datetime", descending=True).head(20)
    else:
        recent = df.tail(20)

    st.dataframe(
        recent.to_pandas(),
        use_container_width=True,
        hide_index=True
    )

    if st.button("Refresh Data"):
        st.rerun()

    auto_refresh = st.checkbox("Auto-refresh every 10 seconds")
    if auto_refresh:
        time.sleep(10)
        st.rerun()

if __name__ == "__main__":
    main()
