# Ray Sentiment Analysis Container
FROM rayproject/ray:2.9.0-py310

# Set working directory
WORKDIR /app

# Install system dependencies
USER root
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Switch to ray user
USER ray

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download NLTK data
RUN python -c "import nltk; nltk.download('vader_lexicon', quiet=True)"

# Copy application files
COPY . .

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV RAY_ADDRESS=ray-head:6379

# Default command
CMD ["python", "ray_worker.py"]
