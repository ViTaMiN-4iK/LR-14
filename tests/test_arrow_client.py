"""
Pytest tests for Arrow and NATS client
"""
import pytest
import sys
import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch, AsyncMock

sys.path.insert(0, str(Path(__file__).parent.parent / "python"))

try:
    from arrow_client import ArrowClient, NATSConsumer, JSONDataProcessor
    ARROW_NATS_AVAILABLE = True
except ImportError:
    ARROW_NATS_AVAILABLE = False


class TestJSONDataProcessor:
    """Tests for JSON data processor (always available)"""

    def test_processor_initialization(self):
        """Test JSON processor initialization"""
        processor = JSONDataProcessor()
        assert processor.data == []
        assert isinstance(processor.data_dir, Path)

    def test_load_from_file_empty(self):
        """Test loading from non-existent file"""
        processor = JSONDataProcessor()
        data = processor.load_from_file("nonexistent.json")
        assert data == []

    def test_load_from_file_success(self):
        """Test loading from existing file"""
        records = [
            {"patient_id": "P001", "sensor_type": "pulse", "value": 75.0},
            {"patient_id": "P002", "sensor_type": "temperature", "value": 36.8},
        ]

        with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False, encoding='utf-8') as f:
            json_path = f.name
            for record in records:
                json.dump(record, f)
                f.write('\n')

        processor = JSONDataProcessor(data_dir=str(Path(json_path).parent))
        data = processor.load_from_file(Path(json_path).name)

        assert len(data) == 2
        assert data[0]["patient_id"] == "P001"

        Path(json_path).unlink()

    def test_get_data(self):
        """Test getting loaded data"""
        processor = JSONDataProcessor()
        processor.data = [{"test": "data"}]
        assert processor.get_data() == [{"test": "data"}]

    def test_clear_data(self):
        """Test clearing data"""
        processor = JSONDataProcessor()
        processor.data = [{"test": "data"}]
        processor.clear_data()
        assert processor.data == []


@pytest.mark.skipif(not ARROW_NATS_AVAILABLE, reason="Arrow/NATS libraries not available")
class TestArrowClient:
    """Tests for Arrow Flight client"""

    def test_client_initialization(self):
        """Test Arrow client initialization"""
        client = ArrowClient(host="localhost", port=8815)
        assert client.host == "localhost"
        assert client.port == 8815
        assert client.data == []

    def test_fetch_without_connection(self):
        """Test fetching data without connection returns empty"""
        client = ArrowClient()
        data = client.fetch_data()
        assert data == []


@pytest.mark.skipif(not ARROW_NATS_AVAILABLE, reason="Arrow/NATS libraries not available")
class TestNATSConsumer:
    """Tests for NATS consumer"""

    def test_consumer_initialization(self):
        """Test NATS consumer initialization"""
        consumer = NATSConsumer()
        assert consumer.data == []
        assert consumer.subscriptions == []

    def test_get_data(self):
        """Test getting received data"""
        consumer = NATSConsumer()
        consumer.data = [{"test": "data"}]
        assert consumer.get_data() == [{"test": "data"}]

    def test_clear_data(self):
        """Test clearing data"""
        consumer = NATSConsumer()
        consumer.data = [{"test": "data"}]
        consumer.clear_data()
        assert consumer.data == []


class TestDataFormat:
    """Tests for data format conversion"""

    def test_json_record_format(self):
        """Test that records have expected format"""
        record = {
            "patient_id": "P001",
            "sensor_type": "pulse",
            "value": 75.0,
            "timestamp": 1234567890.0
        }

        assert "patient_id" in record
        assert "sensor_type" in record
        assert "value" in record
        assert "timestamp" in record

    def test_blood_pressure_format(self):
        """Test blood pressure record format"""
        record = {
            "patient_id": "P001",
            "sensor_type": "blood_pressure",
            "systolic": 120.0,
            "diastolic": 80.0,
            "timestamp": 1234567890.0
        }

        assert "systolic" in record
        assert "diastolic" in record


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
