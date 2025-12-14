"""
Ray operators for Airflow DAG - UPDATED FOR PHASE 4
Now includes actual Ray distributed processing
"""

import logging
import ray
from typing import List, Dict
from datetime import datetime
import sys
sys.path.append('/opt/airflow/dags')

logger = logging.getLogger(__name__)


def ray_sentiment_analysis(**context):
    """
    Process reviews using Ray distributed sentiment analysis
    
    Args:
        context: Airflow context
        
    Returns:
        dict: Processing results
    """
    logger.info("=" * 60)
    logger.info("Task: Ray Distributed Sentiment Analysis")
    logger.info("=" * 60)
    
    # Get reviews from previous task
    ti = context['task_instance']
    reviews = ti.xcom_pull(task_ids='prepare_ray_processing', key='reviews_for_processing')
    
    if not reviews:
        logger.warning("No reviews to process")
        return {'status': 'no_data', 'processed': 0}
    
    logger.info(f"Processing {len(reviews)} reviews with Ray distributed cluster")
    
    try:
        # Import Ray worker (this will be available via mounted volume)
        # For now, we'll use inline implementation
        import ray
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
        
        # Initialize Ray (connect to existing cluster)
        ray_address = "ray-head:6379"
        ray.init(address=ray_address, ignore_reinit_error=True)
        logger.info(f"Connected to Ray cluster at {ray_address}")
        
        # Define remote function for sentiment analysis
        @ray.remote
        def analyze_sentiment_batch(review_batch):
            """Analyze sentiment for a batch of reviews"""
            analyzer = SentimentIntensityAnalyzer()
            results = []
            
            for review in review_batch:
                scores = analyzer.polarity_scores(review['review_text'])
                compound = scores['compound']
                
                # Adjust with rating
                rating = review['rating']
                rating_sentiment = (rating - 3) / 2
                combined_score = 0.7 * compound + 0.3 * rating_sentiment
                
                if combined_score >= 0.05:
                    label = 'POSITIVE'
                elif combined_score <= -0.05:
                    label = 'NEGATIVE'
                else:
                    label = 'NEUTRAL'
                
                result = {
                    **review,
                    'sentiment_label': label,
                    'sentiment_score': round(combined_score, 4),
                    'processed_timestamp': datetime.utcnow().isoformat(),
                    'processing_method': 'ray_distributed'
                }
                results.append(result)
            
            return results
        
        # Split reviews into batches
        num_workers = 2
        batch_size = len(reviews) // num_workers
        if batch_size == 0:
            batch_size = len(reviews)
        
        batches = [reviews[i:i + batch_size] for i in range(0, len(reviews), batch_size)]
        
        logger.info(f"Split into {len(batches)} batches for parallel processing")
        
        # Submit tasks to Ray
        futures = [analyze_sentiment_batch.remote(batch) for batch in batches]
        
        # Collect results
        results = ray.get(futures)
        
        # Flatten results
        processed_reviews = []
        for batch_result in results:
            processed_reviews.extend(batch_result)
        
        # Shutdown Ray connection
        ray.shutdown()
        
        logger.info(f"Processed {len(processed_reviews)} reviews using Ray")
        
        # Calculate distribution
        sentiment_dist = {
            'POSITIVE': sum(1 for r in processed_reviews if r['sentiment_label'] == 'POSITIVE'),
            'NEUTRAL': sum(1 for r in processed_reviews if r['sentiment_label'] == 'NEUTRAL'),
            'NEGATIVE': sum(1 for r in processed_reviews if r['sentiment_label'] == 'NEGATIVE')
        }
        
        logger.info(f"Sentiment distribution: {sentiment_dist}")
        
        # Push results to XCom
        ti.xcom_push(key='processed_reviews', value=processed_reviews)
        
        return {
            'status': 'success',
            'processed': len(processed_reviews),
            'distribution': sentiment_dist,
            'timestamp': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error in Ray processing: {e}")
        import traceback
        traceback.print_exc()
        raise


# Keep other functions as before
def prepare_for_ray_processing(**context):
    """Prepare validated reviews for Ray distributed processing"""
    logger.info("=" * 60)
    logger.info("Task: Prepare for Ray Processing")
    logger.info("=" * 60)
    
    ti = context['task_instance']
    valid_reviews = ti.xcom_pull(task_ids='validate_reviews', key='valid_reviews')
    
    if not valid_reviews:
        logger.warning("No valid reviews to process")
        return {'status': 'no_data', 'count': 0}
    
    logger.info(f"Preparing {len(valid_reviews)} reviews for Ray processing")
    
    formatted_reviews = []
    for review in valid_reviews:
        formatted_reviews.append({
            'review_id': review['review_id'],
            'product_id': review['product_id'],
            'review_text': review['review_text'],
            'rating': review['rating'],
            'batch_timestamp': datetime.utcnow().isoformat()
        })
    
    ti.xcom_push(key='reviews_for_processing', value=formatted_reviews)
    ti.xcom_push(key='processing_batch_size', value=len(formatted_reviews))
    
    metadata = {
        'status': 'ready',
        'count': len(formatted_reviews),
        'prepared_at': datetime.utcnow().isoformat()
    }
    
    logger.info(f"Prepared {len(formatted_reviews)} reviews for Ray")
    return metadata
