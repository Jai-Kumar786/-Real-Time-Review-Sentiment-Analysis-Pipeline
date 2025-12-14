"""
Ray Distributed Sentiment Analysis Worker
Processes reviews in parallel across Ray cluster
"""

import ray
import logging
import time
from typing import List, Dict
from datetime import datetime
from sentiment_analyzer import EnhancedSentimentAnalyzer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@ray.remote
class SentimentWorker:
    """
    Ray actor for distributed sentiment analysis
    Each worker maintains its own analyzer instance
    """
    
    def __init__(self, worker_id: int):
        """
        Initialize worker with sentiment analyzer
        
        Args:
            worker_id: Unique worker identifier
        """
        self.worker_id = worker_id
        self.analyzer = EnhancedSentimentAnalyzer()
        self.processed_count = 0
        logger.info(f"Worker {worker_id} initialized")
    
    def process_review(self, review: Dict) -> Dict:
        """
        Process a single review with sentiment analysis
        
        Args:
            review: Review dictionary with text and rating
            
        Returns:
            dict: Review with sentiment analysis results
        """
        start_time = time.time()
        
        # Extract review data
        review_text = review.get('review_text', '')
        rating = review.get('rating', 3)
        
        # Perform sentiment analysis
        sentiment_result = self.analyzer.analyze_with_rating(review_text, rating)
        
        # Calculate processing time
        processing_time_ms = int((time.time() - start_time) * 1000)
        
        # Combine review with sentiment results
        processed_review = {
            **review,
            **sentiment_result,
            'processed_timestamp': datetime.utcnow().isoformat(),
            'processing_time_ms': processing_time_ms,
            'worker_id': self.worker_id
        }
        
        self.processed_count += 1
        
        return processed_review
    
    def process_batch(self, reviews: List[Dict]) -> List[Dict]:
        """
        Process a batch of reviews
        
        Args:
            reviews: List of review dictionaries
            
        Returns:
            list: List of processed reviews
        """
        logger.info(f"Worker {self.worker_id} processing batch of {len(reviews)} reviews")
        
        processed_reviews = []
        for review in reviews:
            try:
                processed = self.process_review(review)
                processed_reviews.append(processed)
            except Exception as e:
                logger.error(f"Worker {self.worker_id} error processing review: {e}")
                # Add error information
                error_review = {
                    **review,
                    'sentiment_label': 'ERROR',
                    'sentiment_score': 0.0,
                    'error_message': str(e),
                    'worker_id': self.worker_id
                }
                processed_reviews.append(error_review)
        
        logger.info(f"Worker {self.worker_id} completed batch. Total processed: {self.processed_count}")
        return processed_reviews
    
    def get_stats(self) -> Dict:
        """Get worker statistics"""
        return {
            'worker_id': self.worker_id,
            'processed_count': self.processed_count
        }


class RaySentimentProcessor:
    """
    Main Ray sentiment processing coordinator
    Distributes work across multiple workers
    """
    
    def __init__(self, num_workers: int = 2, ray_address: str = None):
        """
        Initialize Ray cluster and workers
        
        Args:
            num_workers: Number of worker actors to create
            ray_address: Ray cluster address (None for local)
        """
        self.num_workers = num_workers
        self.workers = []
        
        # Initialize Ray
        if ray_address:
            ray.init(address=ray_address, ignore_reinit_error=True)
            logger.info(f"Connected to Ray cluster at {ray_address}")
        else:
            ray.init(ignore_reinit_error=True)
            logger.info("Initialized local Ray cluster")
        
        # Create worker actors
        logger.info(f"Creating {num_workers} sentiment workers...")
        for i in range(num_workers):
            worker = SentimentWorker.remote(worker_id=i)
            self.workers.append(worker)
        
        logger.info(f"Ray sentiment processor initialized with {num_workers} workers")
    
    def process_reviews(self, reviews: List[Dict]) -> List[Dict]:
        """
        Process reviews in parallel across workers
        
        Args:
            reviews: List of review dictionaries
            
        Returns:
            list: List of processed reviews with sentiment
        """
        start_time = time.time()
        
        logger.info("=" * 60)
        logger.info(f"Starting distributed processing of {len(reviews)} reviews")
        logger.info(f"Workers: {self.num_workers}")
        logger.info("=" * 60)
        
        # Split reviews into batches for each worker
        batch_size = len(reviews) // self.num_workers
        if batch_size == 0:
            batch_size = 1
        
        batches = [
            reviews[i:i + batch_size] 
            for i in range(0, len(reviews), batch_size)
        ]
        
        # Ensure we don't have more batches than workers
        while len(batches) > self.num_workers:
            # Merge last two batches
            batches[-2].extend(batches[-1])
            batches.pop()
        
        logger.info(f"Split into {len(batches)} batches")
        for i, batch in enumerate(batches):
            logger.info(f"  Batch {i}: {len(batch)} reviews")
        
        # Distribute work to workers
        futures = []
        for i, batch in enumerate(batches):
            worker = self.workers[i % self.num_workers]
            future = worker.process_batch.remote(batch)
            futures.append(future)
        
        logger.info(f"Submitted {len(futures)} tasks to Ray workers")
        
        # Collect results
        results = ray.get(futures)
        
        # Flatten results
        all_processed_reviews = []
        for batch_results in results:
            all_processed_reviews.extend(batch_results)
        
        # Calculate metrics
        processing_time = time.time() - start_time
        throughput = len(all_processed_reviews) / processing_time
        
        logger.info("=" * 60)
        logger.info("Processing Complete")
        logger.info(f"Total reviews processed: {len(all_processed_reviews)}")
        logger.info(f"Total time: {processing_time:.2f} seconds")
        logger.info(f"Throughput: {throughput:.2f} reviews/second")
        logger.info("=" * 60)
        
        # Get worker statistics
        self.print_worker_stats()
        
        return all_processed_reviews
    
    def print_worker_stats(self):
        """Print statistics from all workers"""
        logger.info("\nWorker Statistics:")
        logger.info("-" * 60)
        
        stats_futures = [worker.get_stats.remote() for worker in self.workers]
        stats = ray.get(stats_futures)
        
        for stat in stats:
            logger.info(f"  Worker {stat['worker_id']}: {stat['processed_count']} reviews processed")
    
    def get_sentiment_distribution(self, processed_reviews: List[Dict]) -> Dict:
        """
        Calculate sentiment distribution
        
        Args:
            processed_reviews: List of processed reviews
            
        Returns:
            dict: Distribution statistics
        """
        distribution = {
            'POSITIVE': 0,
            'NEUTRAL': 0,
            'NEGATIVE': 0,
            'ERROR': 0
        }
        
        total_score = 0
        valid_count = 0
        
        for review in processed_reviews:
            label = review.get('sentiment_label', 'ERROR')
            distribution[label] = distribution.get(label, 0) + 1
            
            if label != 'ERROR':
                total_score += review.get('sentiment_score', 0)
                valid_count += 1
        
        avg_score = total_score / valid_count if valid_count > 0 else 0
        
        return {
            'distribution': distribution,
            'total': len(processed_reviews),
            'average_score': round(avg_score, 4)
        }
    
    def shutdown(self):
        """Shutdown Ray cluster"""
        logger.info("Shutting down Ray cluster")
        ray.shutdown()


# Test function
def test_ray_processing():
    """Test Ray sentiment processing with sample data"""
    
    # Sample reviews
    test_reviews = [
        {
            'review_id': f'TEST-{i}',
            'product_id': f'PROD-{i % 5}',
            'review_text': text,
            'rating': rating
        }
        for i, (text, rating) in enumerate([
            ("Absolutely love this product! Best purchase ever!", 5),
            ("Great quality and fast shipping. Highly recommend!", 5),
            ("It's okay, does what it's supposed to do.", 3),
            ("Not impressed. Expected better quality.", 2),
            ("Terrible! Broke after one day. Waste of money!", 1),
            ("Perfect! Exactly what I needed.", 5),
            ("Average product, nothing special.", 3),
            ("Poor quality control. Very disappointed.", 1),
            ("Excellent value for the price!", 4),
            ("Decent, but could be better.", 3)
        ] * 5)  # Multiply to get 50 reviews
    ]
    
    # Initialize processor
    processor = RaySentimentProcessor(num_workers=2)
    
    # Process reviews
    processed = processor.process_reviews(test_reviews)
    
    # Show distribution
    distribution = processor.get_sentiment_distribution(processed)
    
    print("\nSentiment Distribution:")
    print(f"  Positive: {distribution['distribution']['POSITIVE']}")
    print(f"  Neutral: {distribution['distribution']['NEUTRAL']}")
    print(f"  Negative: {distribution['distribution']['NEGATIVE']}")
    print(f"  Average Score: {distribution['average_score']}")
    
    # Shutdown
    processor.shutdown()
    
    return processed


if __name__ == "__main__":
    test_ray_processing()
