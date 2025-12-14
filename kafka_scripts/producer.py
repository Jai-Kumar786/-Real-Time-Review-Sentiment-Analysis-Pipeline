"""
Kafka Producer - Streams product review data to Kafka topic
"""

import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging
from config import KAFKA_TOPIC_REVIEWS, KAFKA_BOOTSTRAP_SERVERS, REVIEWS_PER_BATCH, BATCH_INTERVAL_SECONDS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ReviewProducer:
    """Producer class to stream review data to Kafka"""
    
    def __init__(self):
        """Initialize Kafka producer"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            logger.info(f"Kafka Producer initialized successfully")
            logger.info(f"Connected to: {KAFKA_BOOTSTRAP_SERVERS}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka Producer: {e}")
            raise
    
    def load_sample_reviews(self, filepath='sample_reviews.json'):
        """Load sample reviews from JSON file"""
        try:
            with open(filepath, 'r') as f:
                reviews = json.load(f)
            logger.info(f"Loaded {len(reviews)} sample reviews from {filepath}")
            return reviews
        except FileNotFoundError:
            logger.error(f"Sample reviews file not found: {filepath}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON file: {e}")
            return []
    
    def generate_review_variant(self, base_review):
        """Generate a variant of the base review with updated timestamps and IDs"""
        review = base_review.copy()
        
        # Generate new IDs
        timestamp = int(time.time())
        review['review_id'] = f"R{timestamp}_{random.randint(1000, 9999)}"
        review['customer_id'] = f"C{random.randint(1000, 9999)}"
        
        # Update timestamp to current time with some randomness
        current_time = datetime.now()
        random_offset = timedelta(minutes=random.randint(-60, 0))
        review['review_date'] = (current_time + random_offset).strftime('%Y-%m-%d %H:%M:%S')
        review['timestamp'] = current_time.isoformat()
        
        # Add metadata
        review['source'] = 'kafka_producer'
        review['processed'] = False
        
        return review
    
    def send_review(self, review):
        """Send a single review to Kafka topic"""
        try:
            # Send to Kafka
            future = self.producer.send(
                KAFKA_TOPIC_REVIEWS,
                key=review['review_id'],
                value=review
            )
            
            # Wait for confirmation
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"Sent review {review['review_id']} | "
                f"Product: {review['product_name']} | "
                f"Rating: {review['rating']}/5 | "
                f"Topic: {record_metadata.topic} | "
                f"Partition: {record_metadata.partition} | "
                f"Offset: {record_metadata.offset}"
            )
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to send review {review.get('review_id', 'unknown')}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending review: {e}")
            return False
    
    def stream_reviews(self, continuous=True):
        """
        Stream reviews to Kafka topic
        
        Args:
            continuous: If True, stream indefinitely. If False, send all reviews once.
        """
        sample_reviews = self.load_sample_reviews()
        
        if not sample_reviews:
            logger.error("No sample reviews loaded. Exiting.")
            return
        
        logger.info(f"Starting review stream to topic '{KAFKA_TOPIC_REVIEWS}'")
        logger.info(f"Continuous mode: {continuous}, Interval: {BATCH_INTERVAL_SECONDS}s")
        
        sent_count = 0
        error_count = 0
        
        try:
            while True:
                # Pick a random review from samples
                base_review = random.choice(sample_reviews)
                
                # Generate a variant with new IDs and timestamp
                review = self.generate_review_variant(base_review)
                
                # Send to Kafka
                if self.send_review(review):
                    sent_count += 1
                else:
                    error_count += 1
                
                # Log statistics every 10 messages
                if (sent_count + error_count) % 10 == 0:
                    logger.info(f"Statistics - Sent: {sent_count}, Errors: {error_count}")
                
                # If not continuous, break after one pass through all reviews
                if not continuous and sent_count >= len(sample_reviews):
                    break
                
                # Wait before sending next review
                time.sleep(BATCH_INTERVAL_SECONDS)
                
        except KeyboardInterrupt:
            logger.info("Producer stopped by user")
        except Exception as e:
            logger.error(f"Unexpected error in stream: {e}")
        finally:
            self.close()
            logger.info(f"Final Statistics - Total Sent: {sent_count}, Total Errors: {error_count}")
    
    def close(self):
        """Close the Kafka producer"""
        try:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka Producer closed successfully")
        except Exception as e:
            logger.error(f"Error closing producer: {e}")


def main():
    """Main function to run the producer"""
    logger.info("=" * 60)
    logger.info("Starting Kafka Review Producer")
    logger.info("=" * 60)
    
    try:
        producer = ReviewProducer()
        producer.stream_reviews(continuous=True)
    except Exception as e:
        logger.error(f"Failed to start producer: {e}")
        exit(1)


if __name__ == "__main__":
    main()
