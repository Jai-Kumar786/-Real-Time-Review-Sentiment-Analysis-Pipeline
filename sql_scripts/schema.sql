-- Create reviews table for storing sentiment analysis results
CREATE TABLE IF NOT EXISTS reviews (
    review_id VARCHAR(50) PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    review_text TEXT NOT NULL,
    rating INT CHECK (rating BETWEEN 1 AND 5),
    sentiment_score FLOAT,
    sentiment_label VARCHAR(20),
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processing_time_ms INT
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_sentiment ON reviews(sentiment_label);
CREATE INDEX IF NOT EXISTS idx_product ON reviews(product_id);
CREATE INDEX IF NOT EXISTS idx_timestamp ON reviews(processed_timestamp);

-- Create a summary view
CREATE OR REPLACE VIEW sentiment_summary AS
SELECT 
    sentiment_label,
    COUNT(*) as count,
    AVG(sentiment_score) as avg_score,
    AVG(rating) as avg_rating
FROM reviews
GROUP BY sentiment_label;
