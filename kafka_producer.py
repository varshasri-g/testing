
import json
import time
import random
import os
from datetime import datetime

from confluent_kafka import Producer

# Load environment variables from env.json
def load_env_vars(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)

env_vars = load_env_vars('env.json')

KAFKA_BROKER = os.getenv('KAFKA_BROKER', env_vars.get('KAFKA_BROKER'))
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', env_vars.get('KAFKA_TOPIC'))

if not KAFKA_BROKER or not KAFKA_TOPIC:
    raise ValueError("KAFKA_BROKER and KAFKA_TOPIC must be set either in env.json or as environment variables.")

# Kafka Producer configuration
conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'truck-telematics-producer'
}

producer = Producer(conf)

def generate_telematics_data(vehicle_id):
    speed = random.randint(0, 120)
    latitude = round(random.uniform(30.0, 45.0), 6)
    longitude = round(random.uniform(-120.0, -70.0), 6)
    is_cargo_attached = random.choice([True, False])
    status = random.choice(['online', 'offline'])

    return {
        "vehicle_id": vehicle_id,
        "timestamp": datetime.now().isoformat(),
        "speed": speed,
        "latitude": latitude,
        "longitude": longitude,
        "isCargoAttached": is_cargo_attached,
        "status": status
    }

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

if __name__ == "__main__":
    print(f"Starting Kafka Producer for topic: {KAFKA_TOPIC} on broker: {KAFKA_BROKER}")
    
    vehicle_ids = [f"TRUCK_{i:03d}" for i in range(1, 11)] # Simulate 10 trucks

    try:
        while True:
            for vehicle_id in vehicle_ids:
                data = generate_telematics_data(vehicle_id)
                
                # If speed > 60, generate multiple data points to simulate granular updates
                if data["speed"] > 60:
                    print(f"High speed detected for {vehicle_id}. Generating multiple data points.")
                    for _ in range(random.randint(2, 5)): # Generate 2-5 extra data points
                        # Slightly vary the data for each part
                        part_data = data.copy()
                        part_data["timestamp"] = datetime.now().isoformat()
                        part_data["speed"] = random.randint(61, 120) # Keep speed high
                        part_data["latitude"] = round(part_data["latitude"] + random.uniform(-0.001, 0.001), 6)
                        part_data["longitude"] = round(part_data["longitude"] + random.uniform(-0.001, 0.001), 6)
                        
                        producer.produce(KAFKA_TOPIC, key=vehicle_id.encode('utf-8'), value=json.dumps(part_data).encode('utf-8'), callback=delivery_report)
                        producer.poll(0) # Non-blocking poll to trigger callbacks
                        time.sleep(0.5) # Small delay between parts

                else:
                    producer.produce(KAFKA_TOPIC, key=vehicle_id.encode('utf-8'), value=json.dumps(data).encode('utf-8'), callback=delivery_report)
                    producer.poll(0) # Non-blocking poll to trigger callbacks

            producer.flush() # Ensure all messages are sent
            time.sleep(2) # Wait before generating next batch of data for all trucks

    except KeyboardInterrupt:
        print("Producer stopped by user.")
    finally:
        producer.flush()
        print("Kafka Producer shut down.")
