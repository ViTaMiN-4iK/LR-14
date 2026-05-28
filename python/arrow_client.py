"""
Arrow Flight Client and NATS Consumer for Medical Data
"""
import json
import asyncio
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
import time

try:
    import pyarrow as pa
    import pyarrow.flight as pa_flight
    ARROW_AVAILABLE = True
except ImportError:
    ARROW_AVAILABLE = False
    print("PyArrow not available. Using JSON fallback.")

try:
    import nats
    NATS_AVAILABLE = True
except ImportError:
    NATS_AVAILABLE = False
    print("NATS client not available.")


class ArrowClient:
    """Client for receiving data via Apache Arrow Flight"""

    def __init__(self, host: str = "localhost", port: int = 8815):
        self.host = host
        self.port = port
        self.client = None
        self.data: List[Dict[str, Any]] = []

    def connect(self):
        if not ARROW_AVAILABLE:
            print("PyArrow not available, skipping connection")
            return False

        try:
            self.client = pa_flight.connect(f"grpc://{self.host}:{self.port}")
            print(f"Connected to Arrow Flight server at {self.host}:{self.port}")
            return True
        except Exception as e:
            print(f"Failed to connect to Arrow Flight: {e}")
            return False

    def fetch_data(self) -> List[Dict[str, Any]]:
        if not self.client or not ARROW_AVAILABLE:
            return self.data

        try:
            descriptor = pa_flight.FlightDescriptor.for_path("medical_data")
            reader = self.client.do_get(descriptor)

            table = reader.read_all()
            self.data = table.to_pydict()

            print(f"Received {len(self.data)} records via Arrow Flight")
            return self.data
        except Exception as e:
            print(f"Failed to fetch data: {e}")
            return self.data

    def close(self):
        if self.client:
            self.client.close()


class NATSConsumer:
    """Consumer for receiving data from NATS JetStream"""

    def __init__(self, nats_url: str = "nats://localhost:4222"):
        self.nats_url = nats_url
        self.client = None
        self.data: List[Dict[str, Any]] = []
        self.subscriptions = []

    async def connect(self):
        if not NATS_AVAILABLE:
            print("NATS client not available")
            return False

        try:
            self.client = await nats.connect(self.nats_url)
            print(f"Connected to NATS at {self.nats_url}")
            return True
        except Exception as e:
            print(f"Failed to connect to NATS: {e}")
            return False

    async def subscribe(self, subject: str = "medical.readings"):
        if not self.client:
            print("NATS client not connected")
            return

        async def message_handler(msg):
            try:
                data = json.loads(msg.data.decode())
                data["_received_at"] = datetime.now().isoformat()
                self.data.append(data)
            except Exception as e:
                print(f"Failed to process message: {e}")

        try:
            subscription = await self.client.subscribe(subject, cb=message_handler)
            self.subscriptions.append(subscription)
            print(f"Subscribed to {subject}")
        except Exception as e:
            print(f"Failed to subscribe: {e}")

    async def start_consuming(self):
        print("Starting NATS consumer (use Ctrl+C to stop)...")
        try:
            while True:
                await asyncio.sleep(1)
                if len(self.data) % 100 == 0 and len(self.data) > 0:
                    print(f"Received {len(self.data)} messages so far...")
        except asyncio.CancelledError:
            pass

    async def close(self):
        for sub in self.subscriptions:
            await sub.drain()
        if self.client:
            await self.client.close()

    def get_data(self) -> List[Dict[str, Any]]:
        return self.data

    def clear_data(self):
        self.data = []


class JSONDataProcessor:
    """Process medical data from JSON files"""

    def __init__(self, data_dir: str = "D:/projects/lab 14/data"):
        self.data_dir = Path(data_dir)
        self.data: List[Dict[str, Any]] = []

    def load_from_file(self, filename: str) -> List[Dict[str, Any]]:
        filepath = self.data_dir / filename
        if not filepath.exists():
            print(f"File not found: {filepath}")
            return []

        self.data = []
        with open(filepath, "r") as f:
            for line in f:
                if line.strip():
                    self.data.append(json.loads(line))

        print(f"Loaded {len(self.data)} records from {filename}")
        return self.data

    def get_data(self) -> List[Dict[str, Any]]:
        return self.data

    def clear_data(self):
        """Clear loaded data"""
        self.data = []

    def get_arrow_compatible_data(self):
        """Convert data to PyArrow Table"""
        if not ARROW_AVAILABLE:
            raise RuntimeError("PyArrow not available")

        import pyarrow as pa
        return pa.Table.from_pylist(self.data)


def run_arrow_client_demo():
    """Demonstrate Arrow Flight client"""
    print("=== Arrow Flight Client Demo ===\n")

    client = ArrowClient(host="localhost", port=8815)

    if client.connect():
        data = client.fetch_data()
        print(f"Received data: {data[:3]}...")
        client.close()
    else:
        print("Arrow Flight server not available.")
        print("Start the Go Arrow Flight server to receive data.")


async def run_nats_consumer_demo():
    """Demonstrate NATS consumer"""
    print("=== NATS Consumer Demo ===\n")

    consumer = NATSConsumer()

    if await consumer.connect():
        await consumer.subscribe("medical.readings")

        print("\nWaiting for messages... (Ctrl+C to stop)")
        await consumer.start_consuming()

        await consumer.close()
    else:
        print("NATS server not available.")
        print("Start NATS server and Go NATS publisher to receive data.")


def main():
    print("=== Data Transfer Module ===")
    print("This module receives data from Go collector via:")
    print("  1. Apache Arrow Flight RPC")
    print("  2. NATS JetStream")
    print("  3. JSON file (fallback)")
    print()

    run_arrow_client_demo()

    print("\n" + "="*50 + "\n")

    asyncio.run(run_nats_consumer_demo())


if __name__ == "__main__":
    main()
