# Telematics Data Simulator

This project contains a Python script to simulate live truck telematics data and continuously produce it to a Kafka topic. It's designed to demonstrate streaming applications and can be easily containerized using Docker.

## Features

- Simulates multiple trucks with unique IDs.
- Generates realistic-looking data including speed, latitude, longitude, cargo attachment status, and online/offline status.
- Continuously publishes data to a specified Kafka topic.
- Configurable via environment variables.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

- Docker and Docker Compose (recommended for easy setup of Kafka)
- Python 3.8+
- `confluent-kafka` library (`pip install confluent-kafka`)

### Local Setup (without Docker)

1.  **Start Kafka:**
    You will need a running Kafka instance. The easiest way to get one is using Docker Compose. Create a `docker-compose.yml` file like this:

    ```yaml
    version: '3'
    services:
      zookeeper:
        image: confluentinc/cp-zookeeper:7.0.1
        hostname: zookeeper
        container_name: zookeeper
        ports:
          - "2181:2181"
        environment:
          ZOOKEEPER_CLIENT_PORT: 2181
          ZOOKEEPER_TICK_TIME: 2000

      broker:
        image: confluentinc/cp-kafka:7.0.1
        hostname: broker
        container_name: broker
        ports:
          - "9092:9092"
          - "9093:9093"
        depends_on:
          - zookeeper
        environment:
          KAFKA_BROKER_ID: 1
          KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
          KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
          KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092
          KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
          KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
          KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
          KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
    ```

    Then run:
    ```bash
    docker-compose up -d
    ```

2.  **Install Dependencies:**
    ```bash
    pip install confluent-kafka
    ```

3.  **Configure Environment Variables:**
    Create a `.env` file in the same directory as `telematics_producer.py` or export them directly:

    ```env
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092
    KAFKA_TOPIC=telematics
    NUM_TRUCKS=5
    SIMULATION_INTERVAL_SECONDS=1
    STATUS_CHANGE_PROBABILITY=0.1
    CARGO_CHANGE_PROBABILITY=0.2
    ```

4.  **Run the Producer:**
    ```bash
    python telematics_producer.py
    ```

### Using Docker (Recommended)

1.  **Build the Docker Image:**
    ```bash
    docker build -t telematics-producer .
    ```

2.  **Run with Docker Compose:**
    You can integrate this producer into the `docker-compose.yml` file created earlier. Add the following service:

    ```yaml
    version: '3'
    services:
      # ... (zookeeper and broker services as above)

      telematics-producer:
        build: .
        container_name: telematics-producer
        depends_on:
          - broker
        environment:
          KAFKA_BOOTSTRAP_SERVERS: broker:29092 # Use the internal Docker network address
          KAFKA_TOPIC: telematics
          NUM_TRUCKS: 10
          SIMULATION_INTERVAL_SECONDS: 0.5
          STATUS_CHANGE_PROBABILITY: 0.05
          CARGO_CHANGE_PROBABILITY: 0.1
    ```

    Then run:
    ```bash
    docker-compose up -d
    ```

## Environment Variables

The following environment variables can be used to configure the producer:

-   `KAFKA_BOOTSTRAP_SERVERS`: Comma-separated list of Kafka broker addresses (default: `localhost:9092`).
-   `KAFKA_TOPIC`: The Kafka topic to produce messages to (default: `telematics`).
-   `NUM_TRUCKS`: The number of simulated trucks (default: `5`).
-   `SIMULATION_INTERVAL_SECONDS`: The delay between sending batches of data for all trucks, in seconds (default: `1`).
-   `STATUS_CHANGE_PROBABILITY`: The probability (0.0 to 1.0) of a truck's online/offline status changing in any given interval (default: `0.1`).
-   `CARGO_CHANGE_PROBABILITY`: The probability (0.0 to 1.0) of a truck's cargo attachment status changing in any given interval (default: `0.2`).

## Output Data Format

Each message produced to Kafka is a JSON object with the following structure:

```json
{
    "vehicle_id": "truck_0",
    "speed": 65.23,
    "lat": 34.123456,
    "lng": -117.987654,
    "isCargoAttached": true,
    "status": "online",
    "timestamp": "2023-10-27T10:30:00.123456"
}
```
