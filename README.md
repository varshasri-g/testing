# Kafka Telematics Data Simulator

This project simulates truck telematics data and publishes it to a Kafka topic. It also includes a simple Kafka consumer to demonstrate streaming data processing.

## Project Structure

- `env.json`: Configuration file for Kafka broker and topic.
- `kafka_producer.py`: Python script to simulate telematics data and produce it to Kafka.
- `kafka_consumer.py`: Python script to consume data from the Kafka topic.
- `requirements.txt`: Python dependencies.
- `Dockerfile`: Dockerfile to containerize the application.

## Setup and Run

### Prerequisites

- Docker (recommended for running Kafka and the application)
- Python 3.9+

### 1. Start Kafka (using Docker Compose)

For local development, you can use Docker Compose to set up a Kafka broker. Create a `docker-compose.yml` file in the root of your project with the following content:

```yaml
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.3.0
    hostname: zookeeper
    container_name: zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  broker:
    image: confluentinc/cp-kafka:7.3.0
    hostname: broker
    container_name: broker
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "9093:9093"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
```

Then, start Kafka:

```bash
docker-compose up -d
```

### 2. Configure Environment Variables

Edit `env.json` to match your Kafka broker configuration. By default, it's set to `localhost:9092` and topic `truck_telematics`.

```json
{
    "KAFKA_BROKER": "localhost:9092",
    "KAFKA_TOPIC": "truck_telematics"
}
```

### 3. Run the Kafka Producer

#### Using Python

First, install the required Python packages:

```bash
pip install -r requirements.txt
```

Then, run the producer:

```bash
python kafka_producer.py
```

#### Using Docker

Build the Docker image:

```bash
docker build -t kafka-telematics-producer .
```

Run the producer container:

```bash
docker run --network host kafka-telematics-producer
```

### 4. Run the Kafka Consumer

Open a new terminal for the consumer.

#### Using Python

```bash
python kafka_consumer.py
```

#### Using Docker

Build the Docker image (if not already built, or if you want a separate consumer image):

```bash
docker build -t kafka-telematics-consumer .
```

Run the consumer container:

```bash
docker run --network host kafka-telematics-consumer python kafka_consumer.py
```

This will start consuming messages from the `truck_telematics` topic and print them to the console.

## Logic for High-Speed Vehicles

In `kafka_producer.py`, if a truck's `speed` is greater than 60, the producer will generate an additional 2 to 5 data points for that specific vehicle over a short period (0.5-second intervals). These additional data points will have slightly varied latitude and longitude to simulate continuous movement and will maintain a high speed, providing more granular updates for high-speed events.