# Kafka Truck Telematics Producer

This project contains a Python-based Kafka producer that simulates live truck telematics data and sends it to a specified Kafka topic.

## Features

- Generates synthetic truck data including `vehicle_id`, `speed`, `lat`, `lng`, `isCargoAttached`, and `status`.
- Continuously publishes data to a Kafka topic.
- Configurable Kafka broker and topic via environment variables.
- Includes retry logic with exponential backoff for Kafka connection and message production.
- Basic Dead Letter Queue (DLQ) logging for messages that fail to be produced after multiple retries.

## Prerequisites

- Python 3.9+
- Pip (Python package installer)
- Docker (optional, for containerized deployment)
- A running Kafka broker (e.g., `localhost:9092`)

## Project Structure

```
.  
├── producer.py             # Main Kafka producer script
├── config.json             # Configuration file for Kafka settings
├── Dockerfile              # Dockerfile to containerize the application
├── requirements.txt        # Python dependencies
└── README.md               # This README file
└── env_variables.json      # Definitions for environment variables
```

## Configuration

The Kafka broker address, topic, and message production interval can be configured using environment variables:

- `KAFKA_BROKER`: The Kafka broker address (e.g., `localhost:9092`).
- `KAFKA_TOPIC`: The Kafka topic to produce messages to (e.g., `truck_telematics`).
- `INTERVAL_SECONDS`: The delay in seconds between sending messages (default: `1`).

These variables can be set directly in your shell or via the `config.json` file, which is loaded by default if no environment variables are set.

## How to Run

### 1. Local Execution

#### a. Install Dependencies

```bash
pip install -r requirements.txt
```

#### b. Set Environment Variables (Optional - defaults are provided by config.json and in producer.py)

If you need to override the values in `config.json` or `producer.py` defaults:

```bash
export KAFKA_BROKER="your_kafka_broker:port"
export KAFKA_TOPIC="your_kafka_topic"
export INTERVAL_SECONDS="2"
```

#### c. Run the Producer

```bash
python producer.py
```

### 2. Docker Execution

#### a. Build the Docker Image

Navigate to the project root directory (where `Dockerfile` is located) and run:

```bash
docker build -t truck-telematics-producer .
```

#### b. Run the Docker Container

Make sure your Kafka broker is accessible from within the Docker container. If your Kafka broker is running on `localhost`, you might need to use `host.docker.internal` or the host's IP address if running on Linux.

```bash
docker run -d \
  --name telematics-producer \
  -e KAFKA_BROKER="host.docker.internal:9092" \
  -e KAFKA_TOPIC="truck_telematics" \
  -e INTERVAL_SECONDS="1" \
  truck-telematics-producer
```

Replace `host.docker.internal:9092` with your actual Kafka broker address if it's not accessible via this hostname (common for Linux hosts where you might use `--network="host"` or the host machine's IP).

To stop and remove the container:

```bash
docker stop telematics-producer
docker rm telematics-producer
```

## Viewing Data

You can use a Kafka consumer to view the messages being produced to the `truck_telematics` topic. For example, using `kafka-console-consumer.sh` (part of Kafka distribution):

```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic truck_telematics --from-beginning
```

