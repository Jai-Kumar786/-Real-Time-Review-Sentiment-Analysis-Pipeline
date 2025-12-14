"""
Sentiment Analysis Module
Provides multiple sentiment analysis methods for distributed processing
"""

import logging
from typing import Dict, List
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import nltk

# Download NLTK data (only once)
try:
    nltk.data.find('vader_lexicon')
except LookupError:
    nltk.download('vader_lexicon', quiet=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SentimentAnalyzer:
    """Base sentiment analyzer using VADER (fast, rule-based)"""
    
    def __init__(self):
        """Initialize VADER sentiment analyzer"""
        self.analyzer = SentimentIntensityAnalyzer()
        logger.info("VADER Sentiment Analyzer initialized")
    
    def analyze(self, text: str) -> Dict:
        """
        Analyze sentiment of text using VADER
        
        Args:
            text: Review text to analyze
            
        Returns:
            dict: Sentiment scores and label
        """
        # Get VADER scores
        scores = self.analyzer.polarity_scores(text)
        
        # Determine label based on compound score
        compound = scores['compound']
        
        if compound >= 0.05:
            label = 'POSITIVE'
        elif compound <= -0.05:
            label = 'NEGATIVE'
        else:
            label = 'NEUTRAL'
        
        return {
            'sentiment_label': label,
            'sentiment_score': round(compound, 4),
            'positive': round(scores['pos'], 4),
            'neutral': round(scores['neu'], 4),
            'negative': round(scores['neg'], 4),
            'method': 'vader'
        }
    
    def analyze_batch(self, texts: List[str]) -> List[Dict]:
        """
        Analyze sentiment for batch of texts
        
        Args:
            texts: List of review texts
            
        Returns:
            list: List of sentiment results
        """
        return [self.analyze(text) for text in texts]


class EnhancedSentimentAnalyzer(SentimentAnalyzer):
    """Enhanced analyzer with rating-aware adjustment"""
    
    def analyze_with_rating(self, text: str, rating: int) -> Dict:
        """
        Analyze sentiment with rating context
        
        Args:
            text: Review text
            rating: Star rating (1-5)
            
        Returns:
            dict: Enhanced sentiment analysis
        """
        # Get base VADER analysis
        base_result = self.analyze(text)
        
        # Adjust based on rating
        rating_weight = 0.3  # 30% weight to rating
        text_weight = 0.7    # 70% weight to text analysis
        
        # Convert rating to sentiment score (-1 to 1 scale)
        rating_sentiment = (rating - 3) / 2  # Maps 1→-1, 3→0, 5→1
        
        # Combine text and rating sentiment
        combined_score = (
            text_weight * base_result['sentiment_score'] +
            rating_weight * rating_sentiment
        )
        
        # Determine final label
        if combined_score >= 0.05:
            final_label = 'POSITIVE'
        elif combined_score <= -0.05:
            final_label = 'NEGATIVE'
        else:
            final_label = 'NEUTRAL'
        
        return {
            'sentiment_label': final_label,
            'sentiment_score': round(combined_score, 4),
            'text_sentiment': base_result['sentiment_score'],
            'rating_sentiment': round(rating_sentiment, 4),
            'positive': base_result['positive'],
            'neutral': base_result['neutral'],
            'negative': base_result['negative'],
            'rating': rating,
            'method': 'vader_enhanced'
        }


def get_analyzer(method='vader'):
    """
    Factory function to get sentiment analyzer
    
    Args:
        method: Analysis method ('vader' or 'enhanced')
        
    Returns:
        SentimentAnalyzer instance
    """
    if method == 'enhanced':
        return EnhancedSentimentAnalyzer()
    else:
        return SentimentAnalyzer()


# Test the analyzer
if __name__ == "__main__":
    analyzer = EnhancedSentimentAnalyzer()
    
    # Test samples
    test_reviews = [
        ("This product is absolutely amazing! Best purchase ever!", 5),
        ("It's okay, nothing special but does the job.", 3),
        ("Terrible quality, broke after two days. Very disappointed.", 1),
        ("Great value for money. Highly recommend!", 5),
        ("Not what I expected. Poor quality control.", 2)
    ]
    
    print("=" * 60)
    print("Sentiment Analysis Test")
    print("=" * 60)
    
    for text, rating in test_reviews:
        result = analyzer.analyze_with_rating(text, rating)
        print(f"\nText: {text[:50]}...")
        print(f"Rating: {rating}")
        print(f"Sentiment: {result['sentiment_label']} ({result['sentiment_score']:.3f})")
        print(f"Text Score: {result['text_sentiment']:.3f} | Rating Score: {result['rating_sentiment']:.3f}")
