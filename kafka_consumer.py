
import json
import os

from confluent_kafka import Consumer, KafkaException, KafkaError

# Load environment variables from env.json
def load_env_vars(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)

env_vars = load_env_vars('varsha@MVARSHA:~/Downloads/testing/env.json')

KAFKA_BROKER = os.getenv('KAFKA_BROKER', env_vars.get('KAFKA_BROKER'))
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', env_vars.get('KAFKA_TOPIC'))

if not KAFKA_BROKER or not KAFKA_TOPIC:
    raise ValueError("KAFKA_BROKER and KAFKA_TOPIC must be set either in env.json or as environment variables.")

# Kafka Consumer configuration
conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'truck-telematics-consumer',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

if __name__ == "__main__":
    print(f"Starting Kafka Consumer for topic: {KAFKA_TOPIC} on broker: {KAFKA_BROKER}")
    
    try:
        consumer.subscribe([KAFKA_TOPIC])

        while True:
            msg = consumer.poll(1.0) # Poll for messages with a timeout

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event, not an error
                    print(f"%% {msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}")
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Proper message received
                print(f"Received message: Key={msg.key().decode('utf-8') if msg.key() else 'None'}, Value={json.loads(msg.value().decode('utf-8'))}")

    except KeyboardInterrupt:
        print("Consumer stopped by user.")
    finally:
        consumer.close()
        print("Kafka Consumer shut down.")
