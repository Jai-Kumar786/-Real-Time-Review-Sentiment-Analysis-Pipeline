"""
Kafka Consumer - Consumes review data and forwards to processing pipeline
"""

import json
import time
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import logging
import psycopg2
from psycopg2.extras import execute_values
from config import KAFKA_TOPIC_REVIEWS, KAFKA_BOOTSTRAP_SERVERS, DB_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ReviewConsumer:
    """Consumer class to process review data from Kafka"""
    
    def __init__(self):
        """Initialize Kafka consumer and database connection"""
        try:
            self.consumer = KafkaConsumer(
                KAFKA_TOPIC_REVIEWS,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='review-consumer-group',
                max_poll_records=100
            )
            logger.info(f"Kafka Consumer initialized successfully")
            logger.info(f"Subscribed to topic: {KAFKA_TOPIC_REVIEWS}")
            logger.info(f"Consumer group: review-consumer-group")
            
            # Initialize database connection
            self.db_conn = None
            self.connect_to_database()
            
        except Exception as e:
            logger.error(f"Failed to initialize Kafka Consumer: {e}")
            raise
    
    def connect_to_database(self):
        """Establish connection to PostgreSQL database"""
        max_retries = 5
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                self.db_conn = psycopg2.connect(**DB_CONFIG)
                logger.info("Successfully connected to PostgreSQL database")
                self.create_table_if_not_exists()
                return
            except psycopg2.OperationalError as e:
                logger.warning(f"Database connection attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error("Failed to connect to database after all retries")
                    raise
    
    def create_table_if_not_exists(self):
        """Create reviews table if it doesn't exist"""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS raw_reviews (
            id SERIAL PRIMARY KEY,
            review_id VARCHAR(100) UNIQUE NOT NULL,
            product_id VARCHAR(100) NOT NULL,
            product_name VARCHAR(500),
            customer_id VARCHAR(100),
            rating INTEGER,
            review_text TEXT,
            review_date TIMESTAMP,
            verified_purchase BOOLEAN,
            source VARCHAR(100),
            processed BOOLEAN DEFAULT FALSE,
            sentiment_score FLOAT,
            sentiment_label VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_review_id ON raw_reviews(review_id);
        CREATE INDEX IF NOT EXISTS idx_product_id ON raw_reviews(product_id);
        CREATE INDEX IF NOT EXISTS idx_processed ON raw_reviews(processed);
        CREATE INDEX IF NOT EXISTS idx_created_at ON raw_reviews(created_at);
        """
        
        try:
            with self.db_conn.cursor() as cursor:
                cursor.execute(create_table_query)
                self.db_conn.commit()
                logger.info("Database table 'raw_reviews' is ready")
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            self.db_conn.rollback()
    
    def save_to_database(self, review):
        """Save review to PostgreSQL database"""
        insert_query = """
        INSERT INTO raw_reviews (
            review_id, product_id, product_name, customer_id, 
            rating, review_text, review_date, verified_purchase, 
            source, processed
        ) VALUES (
            %(review_id)s, %(product_id)s, %(product_name)s, %(customer_id)s,
            %(rating)s, %(review_text)s, %(review_date)s, %(verified_purchase)s,
            %(source)s, %(processed)s
        )
        ON CONFLICT (review_id) DO UPDATE SET
            updated_at = CURRENT_TIMESTAMP,
            rating = EXCLUDED.rating,
            review_text = EXCLUDED.review_text
        RETURNING id;
        """
        
        try:
            with self.db_conn.cursor() as cursor:
                cursor.execute(insert_query, review)
                review_db_id = cursor.fetchone()[0]
                self.db_conn.commit()
                return review_db_id
        except Exception as e:
            logger.error(f"Error saving review to database: {e}")
            self.db_conn.rollback()
            return None
    
    def process_message(self, message):
        """Process a single Kafka message"""
        try:
            # Get the review from message value
            review = message.value
            
            if not review:
                logger.warning(f"Skipping invalid message at offset {message.offset}")
                return False
            
            # Save to database
            db_id = self.save_to_database(review)
            
            if db_id:
                logger.info(
                    f"Processed review {review['review_id']} | "
                    f"Product: {review['product_name']} | "
                    f"Rating: {review['rating']}/5 | "
                    f"DB ID: {db_id} | "
                    f"Partition: {message.partition} | "
                    f"Offset: {message.offset}"
                )
                return True
            else:
                logger.error(f"Failed to save review {review['review_id']} to database")
                return False
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False
    
    def consume_reviews(self):
        """Consume and process reviews from Kafka topic"""
        logger.info(f"Starting to consume messages from topic '{KAFKA_TOPIC_REVIEWS}'")
        
        processed_count = 0
        error_count = 0
        
        try:
            for message in self.consumer:
                if self.process_message(message):
                    processed_count += 1
                else:
                    error_count += 1
                
                # Log statistics every 10 messages
                if (processed_count + error_count) % 10 == 0:
                    logger.info(
                        f"Statistics - Processed: {processed_count}, "
                        f"Errors: {error_count}"
                    )
                    
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        except Exception as e:
            logger.error(f"Unexpected error in consumer: {e}")
        finally:
            self.close()
            logger.info(
                f"Final Statistics - Total Processed: {processed_count}, "
                f"Total Errors: {error_count}"
            )
    
    def close(self):
        """Close Kafka consumer and database connection"""
        try:
            self.consumer.close()
            logger.info("Kafka Consumer closed successfully")
        except Exception as e:
            logger.error(f"Error closing consumer: {e}")
        
        try:
            if self.db_conn:
                self.db_conn.close()
                logger.info("Database connection closed successfully")
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")


def main():
    """Main function to run the consumer"""
    logger.info("=" * 60)
    logger.info("Starting Kafka Review Consumer")
    logger.info("=" * 60)
    
    try:
        consumer = ReviewConsumer()
        consumer.consume_reviews()
    except Exception as e:
        logger.error(f"Failed to start consumer: {e}")
        exit(1)


if __name__ == "__main__":
    main()
