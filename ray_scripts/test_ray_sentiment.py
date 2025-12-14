"""
Test script for Ray sentiment analysis
"""

import ray
import sys
import logging
from ray_worker import RaySentimentProcessor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Run Ray sentiment analysis test"""
    
    logger.info("=" * 70)
    logger.info("Ray Distributed Sentiment Analysis - Test Run")
    logger.info("=" * 70)
    
    # Check if Ray head is accessible
    ray_address = "ray-head:6379"
    
    try:
        # Test reviews
        test_reviews = [
            {
                'review_id': f'TEST-{i:04d}',
                'product_id': f'PROD-{(i % 10):03d}',
                'review_text': text,
                'rating': rating
            }
            for i, (text, rating) in enumerate([
                ("Outstanding quality! Exceeded all expectations.", 5),
                ("Great product, very satisfied with purchase.", 5),
                ("Good value for money. Recommend it.", 4),
                ("It's okay, meets basic requirements.", 3),
                ("Not what I expected. Disappointed.", 2),
                ("Terrible quality. Complete waste of money!", 1),
                ("Amazing! Best product I've ever bought!", 5),
                ("Decent product, nothing extraordinary.", 3),
                ("Poor quality. Would not recommend.", 2),
                ("Perfect! Exactly what I was looking for.", 5),
            ] * 10)  # 100 reviews total
        ]
        
        logger.info(f"Test dataset: {len(test_reviews)} reviews")
        
        # Initialize processor
        logger.info(f"Connecting to Ray cluster at {ray_address}")
        processor = RaySentimentProcessor(
            num_workers=2,
            ray_address=ray_address
        )
        
        # Process reviews
        processed_reviews = processor.process_reviews(test_reviews)
        
        # Print results
        logger.info("\n" + "=" * 70)
        logger.info("Results Summary")
        logger.info("=" * 70)
        
        distribution = processor.get_sentiment_distribution(processed_reviews)
        
        logger.info(f"\nTotal Processed: {distribution['total']}")
        logger.info(f"Average Sentiment Score: {distribution['average_score']}")
        logger.info(f"\nDistribution:")
        logger.info(f"  ✅ Positive: {distribution['distribution']['POSITIVE']}")
        logger.info(f"  ⚖️  Neutral:  {distribution['distribution']['NEUTRAL']}")
        logger.info(f"  ❌ Negative: {distribution['distribution']['NEGATIVE']}")
        
        if distribution['distribution']['ERROR'] > 0:
            logger.warning(f"  ⚠️  Errors:   {distribution['distribution']['ERROR']}")
        
        # Show sample results
        logger.info("\n" + "=" * 70)
        logger.info("Sample Processed Reviews (first 5)")
        logger.info("=" * 70)
        
        for i, review in enumerate(processed_reviews[:5]):
            logger.info(f"\n{i+1}. Review ID: {review['review_id']}")
            logger.info(f"   Text: {review['review_text'][:60]}...")
            logger.info(f"   Rating: {review['rating']}")
            logger.info(f"   Sentiment: {review['sentiment_label']} ({review['sentiment_score']:.3f})")
            logger.info(f"   Worker: {review['worker_id']} | Time: {review['processing_time_ms']}ms")
        
        # Shutdown
        processor.shutdown()
        
        logger.info("\n" + "=" * 70)
        logger.info("✅ Test completed successfully!")
        logger.info("=" * 70)
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
