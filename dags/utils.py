"""
Utility functions for the review pipeline DAG
"""

import logging
from datetime import datetime
import json

logger = logging.getLogger(__name__)


def validate_review_batch(reviews):
    """
    Validate a batch of reviews
    
    Args:
        reviews: List of review dictionaries
        
    Returns:
        tuple: (valid_reviews, invalid_count)
    """
    valid_reviews = []
    invalid_count = 0
    
    required_fields = ['review_id', 'product_id', 'review_text', 'rating', 'timestamp']
    
    for review in reviews:
        # Check all required fields exist
        if all(field in review for field in required_fields):
            # Validate rating range
            if isinstance(review['rating'], int) and 1 <= review['rating'] <= 5:
                # Validate review text is not empty
                if review['review_text'] and len(review['review_text'].strip()) > 0:
                    valid_reviews.append(review)
                else:
                    invalid_count += 1
            else:
                invalid_count += 1
        else:
            invalid_count += 1
    
    logger.info(f"Validation complete: {len(valid_reviews)} valid, {invalid_count} invalid")
    return valid_reviews, invalid_count


def format_review_for_processing(review):
    """
    Format review data for Ray processing
    
    Args:
        review: Review dictionary
        
    Returns:
        dict: Formatted review
    """
    return {
        'review_id': review['review_id'],
        'product_id': review['product_id'],
        'review_text': review['review_text'],
        'rating': review['rating'],
        'original_timestamp': review['timestamp'],
        'processing_started_at': datetime.utcnow().isoformat()
    }


def calculate_pipeline_metrics(start_time, end_time, reviews_processed):
    """
    Calculate pipeline performance metrics
    
    Args:
        start_time: Pipeline start timestamp
        end_time: Pipeline end timestamp
        reviews_processed: Number of reviews processed
        
    Returns:
        dict: Metrics dictionary
    """
    duration = (end_time - start_time).total_seconds()
    
    metrics = {
        'duration_seconds': duration,
        'reviews_processed': reviews_processed,
        'throughput_per_second': reviews_processed / duration if duration > 0 else 0,
        'start_time': start_time.isoformat(),
        'end_time': end_time.isoformat()
    }
    
    return metrics
