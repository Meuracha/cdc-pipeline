FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --timeout=300 --retries=10 duckdb
RUN pip install --no-cache-dir --timeout=120 --retries=5 confluent-kafka

COPY consumers/sink_duckdb.py .

CMD ["python", "sink_duckdb.py"]