FROM python:3.9-slim-buster

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .
COPY config.json .

ENV KAFKA_BROKER="localhost:9092"
ENV KAFKA_TOPIC="truck_telematics"
ENV INTERVAL_SECONDS="1"

CMD ["python", "producer.py"]
