
import os
import json
import time
import random
from kafka import KafkaProducer
from loguru import logger

# Configure logger
logger.add("file.log", rotation="500 MB", level="INFO")

def get_env_variable(name, default_value=None):
    """Get environment variable or raise an exception."""
    value = os.getenv(name)
    if value is None and default_value is None:
        logger.error(f"Environment variable '{name}' not set.")
        raise ValueError(f"Environment variable '{name}' not set.")
    return value if value is not None else default_value

def generate_telematics_data(vehicle_id_prefix="truck", num_trucks=10):
    """Generates simulated truck telematics data."""
    vehicle_id = f"{vehicle_id_prefix}-{random.randint(1, num_trucks):03d}"
    speed = random.uniform(0, 120)  # km/h
    lat = random.uniform(34.0, 35.0)  # Latitude near a city
    lng = random.uniform(-118.0, -117.0) # Longitude near a city
    is_cargo_attached = random.choice([True, False])
    status = random.choice(["online", "offline"])

    return {
        "vehicle_id": vehicle_id,
        "timestamp": int(time.time() * 1000),
        "speed": round(speed, 2),
        "lat": round(lat, 6),
        "lng": round(lng, 6),
        "isCargoAttached": is_cargo_attached,
        "status": status
    }

def create_kafka_producer(bootstrap_servers):
    """Creates and returns a KafkaProducer instance with retry logic."""
    producer = None
    retries = 0
    max_retries = 5
    while producer is None and retries < max_retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',  # Ensure all replicas acknowledge
                retries=5,   # Number of retries for sending failed messages
                retry_backoff_ms=1000 # 1 second backoff
            )
            logger.info(f"Kafka producer created successfully for brokers: {bootstrap_servers}")
            return producer
        except Exception as e:
            retries += 1
            logger.error(f"Error creating Kafka producer: {e}. Retrying in {2**retries} seconds... (Attempt {retries}/{max_retries})")
            time.sleep(2**retries) # Exponential backoff
    raise ConnectionError("Failed to connect to Kafka brokers after multiple retries.")

def send_message(producer, topic, message, retries=3, backoff_factor=2):
    """Sends a message to Kafka with retry and dead-letter handling."""
    for attempt in range(retries):
        try:
            future = producer.send(topic, message)
            record_metadata = future.get(timeout=10) # Block until send is complete
            logger.info(f"Sent message to topic '{record_metadata.topic}', partition {record_metadata.partition}, offset {record_metadata.offset}: {message['vehicle_id']}")
            return True
        except Exception as e:
            if attempt < retries - 1:
                sleep_time = backoff_factor ** attempt
                logger.warning(f"Failed to send message on attempt {attempt + 1}/{retries}: {e}. Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                logger.error(f"Failed to send message after {retries} attempts. Moving to DLQ (logging for now): {message}. Error: {e}")
                # In a real application, you would send this to a Dead Letter Topic (DLT)
                return False
    return False

def main():
    bootstrap_servers = get_env_variable("KAFKA_BROKER", "localhost:9092")
    kafka_topic = get_env_variable("KAFKA_TOPIC", "truck_telematics")
    interval_seconds = int(get_env_variable("INTERVAL_SECONDS", "1"))

    producer = create_kafka_producer(bootstrap_servers)

    logger.info(f"Starting Kafka producer for topic '{kafka_topic}' on brokers '{bootstrap_servers}'")

    try:
        while True:
            telematics_data = generate_telematics_data()
            send_message(producer, kafka_topic, telematics_data)
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        logger.info("Producer stopped by user.")
    finally:
        if producer:
            producer.flush()
            producer.close()
            logger.info("Kafka producer closed.")

if __name__ == "__main__":
    main()
