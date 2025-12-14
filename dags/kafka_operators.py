"""
Custom Kafka operators for Airflow DAG
"""

import logging
from typing import List, Dict
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import json
from datetime import datetime

logger = logging.getLogger(__name__)


class KafkaReviewFetcher:
    """Fetches reviews from Kafka topic"""
    
    def __init__(self, bootstrap_servers='kafka:9092', topic='review-stream'):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.consumer = None
    
    def fetch_batch(self, max_messages=100, timeout_ms=10000):
        """
        Fetch a batch of reviews from Kafka
        
        Args:
            max_messages: Maximum number of messages to fetch
            timeout_ms: Timeout in milliseconds
            
        Returns:
            List of review dictionaries
        """
        reviews = []
        
        try:
            # Create consumer
            consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                group_id=f'airflow-dag-{datetime.utcnow().strftime("%Y%m%d%H%M%S")}',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=timeout_ms
            )
            
            logger.info(f"Fetching up to {max_messages} messages from topic '{self.topic}'")
            
            # Fetch messages
            for message in consumer:
                reviews.append(message.value)
                
                if len(reviews) >= max_messages:
                    break
            
            # Commit offsets
            consumer.commit()
            consumer.close()
            
            logger.info(f"Fetched {len(reviews)} reviews from Kafka")
            return reviews
            
        except KafkaError as e:
            logger.error(f"Error fetching from Kafka: {e}")
            if consumer:
                consumer.close()
            raise
        
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            if consumer:
                consumer.close()
            raise


def fetch_reviews_from_kafka(**context):
    """
    Airflow task function to fetch reviews from Kafka
    
    Args:
        context: Airflow context
        
    Returns:
        List of reviews
    """
    logger.info("=" * 60)
    logger.info("Task: Fetch Reviews from Kafka")
    logger.info("=" * 60)
    
    # Get parameters from DAG config
    dag_config = context.get('params', {})
    max_messages = dag_config.get('max_reviews_per_run', 100)
    
    # Create fetcher
    fetcher = KafkaReviewFetcher()
    
    # Fetch reviews
    reviews = fetcher.fetch_batch(max_messages=max_messages)
    
    # Push to XCom for next task
    context['task_instance'].xcom_push(key='raw_reviews', value=reviews)
    context['task_instance'].xcom_push(key='review_count', value=len(reviews))
    
    logger.info(f"Pushed {len(reviews)} reviews to XCom")
    
    return len(reviews)
