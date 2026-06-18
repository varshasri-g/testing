
import os
import json
import time
import random
from datetime import datetime
from confluent_kafka import Producer

# Kafka configuration from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'telematics')
NUM_TRUCKS = int(os.getenv('NUM_TRUCKS', '5'))
SIMULATION_INTERVAL_SECONDS = int(os.getenv('SIMULATION_INTERVAL_SECONDS', '1'))
STATUS_CHANGE_PROBABILITY = float(os.getenv('STATUS_CHANGE_PROBABILITY', '0.1'))
CARGO_CHANGE_PROBABILITY = float(os.getenv('CARGO_CHANGE_PROBABILITY', '0.2'))


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def generate_telematics_data(truck_id, current_data=None):
    """Generates simulated telematics data for a truck."""
    if current_data is None:
        # Initial data for a new truck
        latitude = random.uniform(34.0, 35.0)
        longitude = random.uniform(-118.0, -117.0)
        speed = random.uniform(0, 100)
        is_cargo_attached = random.choice([True, False])
        status = random.choice(['online', 'offline'])
    else:
        # Update existing data
        latitude = current_data['lat'] + random.uniform(-0.01, 0.01)
        longitude = current_data['lng'] + random.uniform(-0.01, 0.01)
        speed = max(0, current_data['speed'] + random.uniform(-10, 10)) # Speed can fluctuate

        # Randomly change cargo status
        is_cargo_attached = current_data['isCargoAttached']
        if random.random() < CARGO_CHANGE_PROBABILITY:
            is_cargo_attached = not is_cargo_attached

        # Randomly change status (online/offline)
        status = current_data['status']
        if random.random() < STATUS_CHANGE_PROBABILITY:
            status = 'offline' if status == 'online' else 'online'

    timestamp = datetime.now().isoformat()

    return {
        'vehicle_id': f'truck_{truck_id}',
        'speed': round(speed, 2),
        'lat': round(latitude, 6),
        'lng': round(longitude, 6),
        'isCargoAttached': is_cargo_attached,
        'status': status,
        'timestamp': timestamp
    }


def simulate_telematics():
    """Simulates telematics data and produces it to Kafka."""
    producer_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
    producer = Producer(producer_conf)

    trucks_data = {}
    for i in range(NUM_TRUCKS):
        trucks_data[f'truck_{i}'] = generate_telematics_data(i)

    print(f"Starting telematics simulation for {NUM_TRUCKS} trucks. Producing to topic: {KAFKA_TOPIC}")

    try:
        while True:
            for truck_id, data in list(trucks_data.items()): # Use list() to allow modification during iteration
                updated_data = generate_telematics_data(truck_id, data)
                trucks_data[truck_id] = updated_data # Update the stored data

                # Produce to Kafka
                try:
                    producer.produce(
                        KAFKA_TOPIC,
                        key=updated_data['vehicle_id'].encode('utf-8'),
                        value=json.dumps(updated_data).encode('utf-8'),
                        callback=delivery_report
                    )
                    producer.poll(0)  # Serve delivery reports immediately
                except Exception as e:
                    print(f"Failed to produce message for truck {truck_id}: {e}")

            producer.flush()
            time.sleep(SIMULATION_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("Simulation stopped by user.")
    finally:
        print("Flushing final messages...")
        producer.flush()


if __name__ == "__main__":
    simulate_telematics()
