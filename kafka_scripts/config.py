"""
Kafka Configuration for MLOps PoC
Shared settings for producer and consumer
"""
import os
import json

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC_REVIEWS = os.getenv('KAFKA_TOPIC_REVIEWS', 'review-stream')
KAFKA_TOPIC_PROCESSED = os.getenv('KAFKA_TOPIC_PROCESSED', 'processed-reviews')

# Producer Configuration
PRODUCER_CONFIG = {
    'bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS,
    'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
    'key_serializer': lambda k: k.encode('utf-8') if k else None,
    'acks': 'all',  # Wait for all replicas
    'retries': 3,
    'max_in_flight_requests_per_connection': 1
}

# Consumer Configuration
CONSUMER_CONFIG = {
    'bootstrap_servers': KAFKA_BOOTSTRAP_SERVERS,
    'value_deserializer': lambda m: json.loads(m.decode('utf-8')),
    'key_deserializer': lambda k: k.decode('utf-8') if k else None,
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': True,
    'group_id': 'review-consumer-group',
    'max_poll_records': 100
}

# Database Configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DB', 'airflow'),
    'user': os.getenv('POSTGRES_USER', 'airflow'),
    'password': os.getenv('POSTGRES_PASSWORD', 'airflow')
}

# Data Generation Settings
REVIEWS_PER_BATCH = int(os.getenv('REVIEWS_PER_BATCH', 10))
BATCH_INTERVAL_SECONDS = int(os.getenv('BATCH_INTERVAL_SECONDS', 5))
