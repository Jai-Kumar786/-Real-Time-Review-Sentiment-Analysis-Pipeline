"""
Review Sentiment Analysis Pipeline DAG
MLOps DataOps PoC - Main Orchestration

Data Flow:
1. Fetch reviews from Kafka topic
2. Validate review data quality
3. Prepare data for distributed processing
4. Process with Ray (sentiment analysis)
5. Store results in PostgreSQL
6. Send completion notification
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

# Import custom modules
from kafka_operators import fetch_reviews_from_kafka
from ray_operators import prepare_for_ray_processing, simulate_ray_sentiment_analysis
from utils import validate_review_batch

logger = logging.getLogger(__name__)

# ======================== DAG Configuration ========================

default_args = {
    'owner': 'dataops_team',
    'depends_on_past': False,
    'email': ['dataops@mlops-poc.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30)
}

# DAG parameters
dag_params = {
    'max_reviews_per_run': 100,
    'kafka_topic': 'review-stream',
    'kafka_bootstrap_servers': 'kafka:9092'
}

# ======================== Task Functions ========================

def validate_reviews_task(**context):
    """
    Validate reviews fetched from Kafka
    
    Args:
        context: Airflow context
        
    Returns:
        dict: Validation results
    """
    logger.info("=" * 60)
    logger.info("Task: Validate Reviews")
    logger.info("=" * 60)
    
    # Get reviews from previous task
    ti = context['task_instance']
    raw_reviews = ti.xcom_pull(task_ids='fetch_kafka_reviews', key='raw_reviews')
    
    if not raw_reviews:
        logger.warning("No reviews to validate")
        return {'valid': 0, 'invalid': 0, 'status': 'no_data'}
    
    logger.info(f"Validating {len(raw_reviews)} reviews")
    
    # Validate reviews
    valid_reviews, invalid_count = validate_review_batch(raw_reviews)
    
    # Push to XCom
    ti.xcom_push(key='valid_reviews', value=valid_reviews)
    ti.xcom_push(key='valid_count', value=len(valid_reviews))
    ti.xcom_push(key='invalid_count', value=invalid_count)
    
    validation_rate = (len(valid_reviews) / len(raw_reviews)) * 100 if raw_reviews else 0
    
    logger.info(f"Validation complete: {len(valid_reviews)} valid, {invalid_count} invalid")
    logger.info(f"Validation rate: {validation_rate:.2f}%")
    
    return {
        'valid': len(valid_reviews),
        'invalid': invalid_count,
        'rate': validation_rate,
        'status': 'success'
    }


def store_results_task(**context):
    """
    Store processed reviews in PostgreSQL
    
    Args:
        context: Airflow context
        
    Returns:
        dict: Storage results
    """
    logger.info("=" * 60)
    logger.info("Task: Store Results in PostgreSQL")
    logger.info("=" * 60)
    
    # Get processed reviews from previous task
    ti = context['task_instance']
    processed_reviews = ti.xcom_pull(task_ids='ray_sentiment_analysis', key='processed_reviews')
    
    if not processed_reviews:
        logger.warning("No processed reviews to store")
        return {'status': 'no_data', 'stored': 0}
    
    logger.info(f"Storing {len(processed_reviews)} processed reviews")
    
    # Connect to PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    
    # Insert reviews
    stored_count = 0
    failed_count = 0
    
    insert_query = """
        INSERT INTO reviews (
            review_id, product_id, review_text, rating,
            sentiment_score, sentiment_label, processed_timestamp
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (review_id) DO UPDATE SET
            sentiment_score = EXCLUDED.sentiment_score,
            sentiment_label = EXCLUDED.sentiment_label,
            processed_timestamp = EXCLUDED.processed_timestamp
    """
    
    for review in processed_reviews:
        try:
            cursor.execute(insert_query, (
                review['review_id'],
                review['product_id'],
                review['review_text'],
                review['rating'],
                review['sentiment_score'],
                review['sentiment_label'],
                review['processed_timestamp']
            ))
            stored_count += 1
        except Exception as e:
            logger.error(f"Error storing review {review['review_id']}: {e}")
            failed_count += 1
    
    # Commit transaction
    conn.commit()
    cursor.close()
    conn.close()
    
    logger.info(f"Storage complete: {stored_count} stored, {failed_count} failed")
    
    # Push summary to XCom
    ti.xcom_push(key='stored_count', value=stored_count)
    ti.xcom_push(key='failed_count', value=failed_count)
    
    return {
        'status': 'success',
        'stored': stored_count,
        'failed': failed_count
    }


def generate_pipeline_summary(**context):
    """
    Generate pipeline execution summary
    
    Args:
        context: Airflow context
        
    Returns:
        dict: Pipeline summary
    """
    logger.info("=" * 60)
    logger.info("Task: Generate Pipeline Summary")
    logger.info("=" * 60)
    
    ti = context['task_instance']
    
    # Collect metrics from all tasks
    fetched_count = ti.xcom_pull(task_ids='fetch_kafka_reviews', key='review_count') or 0
    valid_count = ti.xcom_pull(task_ids='validate_reviews', key='valid_count') or 0
    invalid_count = ti.xcom_pull(task_ids='validate_reviews', key='invalid_count') or 0
    processed_count = ti.xcom_pull(task_ids='ray_sentiment_analysis', key='processed_reviews')
    processed_count = len(processed_count) if processed_count else 0
    stored_count = ti.xcom_pull(task_ids='store_results', key='stored_count') or 0
    
    summary = {
        'pipeline_run': context['dag_run'].run_id,
        'execution_date': context['execution_date'].isoformat(),
        'metrics': {
            'fetched_from_kafka': fetched_count,
            'valid_reviews': valid_count,
            'invalid_reviews': invalid_count,
            'processed_reviews': processed_count,
            'stored_in_db': stored_count
        },
        'validation_rate': (valid_count / fetched_count * 100) if fetched_count > 0 else 0,
        'storage_rate': (stored_count / processed_count * 100) if processed_count > 0 else 0,
        'status': 'success'
    }
    
    logger.info("Pipeline Summary:")
    logger.info(f"  Fetched from Kafka: {fetched_count}")
    logger.info(f"  Valid reviews: {valid_count}")
    logger.info(f"  Invalid reviews: {invalid_count}")
    logger.info(f"  Processed reviews: {processed_count}")
    logger.info(f"  Stored in DB: {stored_count}")
    logger.info(f"  Validation rate: {summary['validation_rate']:.2f}%")
    logger.info(f"  Storage rate: {summary['storage_rate']:.2f}%")
    
    return summary


# ======================== DAG Definition ========================

with DAG(
    dag_id='review_sentiment_pipeline',
    default_args=default_args,
    description='End-to-end review sentiment analysis pipeline with Kafka, Ray, and PostgreSQL',
    schedule_interval='*/10 * * * *',  # Run every 10 minutes
    start_date=datetime(2025, 12, 14),
    catchup=False,
    tags=['mlops', 'kafka', 'ray', 'sentiment-analysis', 'poc'],
    params=dag_params,
    max_active_runs=1
) as dag:
    
    # ==================== Task 1: Fetch from Kafka ====================
    fetch_reviews = PythonOperator(
        task_id='fetch_kafka_reviews',
        python_callable=fetch_reviews_from_kafka,
        provide_context=True,
        doc_md="""
        ### Fetch Reviews from Kafka
        
        Fetches a batch of reviews from the Kafka 'review-stream' topic.
        - Max reviews per run: 100
        - Timeout: 10 seconds
        - Pushes reviews to XCom for downstream tasks
        """
    )
    
    # ==================== Task 2: Validate Reviews ====================
    validate_reviews = PythonOperator(
        task_id='validate_reviews',
        python_callable=validate_reviews_task,
        provide_context=True,
        doc_md="""
        ### Validate Review Data
        
        Validates fetched reviews for:
        - Required fields presence
        - Rating range (1-5)
        - Non-empty review text
        """
    )
    
    # ==================== Task 3: Prepare for Ray ====================
    prepare_ray = PythonOperator(
        task_id='prepare_ray_processing',
        python_callable=prepare_for_ray_processing,
        provide_context=True,
        doc_md="""
        ### Prepare for Ray Processing
        
        Formats validated reviews for distributed processing with Ray.
        """
    )
    
    # ==================== Task 4: Ray Sentiment Analysis ====================
    ray_processing = PythonOperator(
    task_id='ray_sentiment_analysis',
    python_callable=ray_sentiment_analysis,  # NEW - actual Ray processing
    provide_context=True,
    doc_md="""
    ### Ray Distributed Sentiment Analysis
    
    Processes reviews using Ray distributed computing cluster.
    - Connects to Ray cluster at ray-head:6379
    - Distributes work across 2 worker nodes
    - Uses VADER sentiment analysis with rating adjustment
    """
)

    
    # ==================== Task 5: Store Results ====================
    store_results = PythonOperator(
        task_id='store_results',
        python_callable=store_results_task,
        provide_context=True,
        doc_md="""
        ### Store Results in PostgreSQL
        
        Stores processed reviews with sentiment scores in PostgreSQL database.
        """
    )
    
    # ==================== Task 6: Generate Summary ====================
    generate_summary = PythonOperator(
        task_id='generate_summary',
        python_callable=generate_pipeline_summary,
        provide_context=True,
        doc_md="""
        ### Generate Pipeline Summary
        
        Collects metrics from all tasks and generates execution summary.
        """
    )
    
    # ==================== Task 7: Notification ====================
    notify = BashOperator(
        task_id='send_notification',
        bash_command="""
        echo "=========================================="
        echo "Review Pipeline Execution Complete"
        echo "DAG Run: {{ dag_run.run_id }}"
        echo "Execution Date: {{ ds }}"
        echo "=========================================="
        """,
        doc_md="""
        ### Send Notification
        
        Sends completion notification (currently just logs).
        Can be extended to email, Slack, etc.
        """
    )
    
    # ==================== Task Dependencies ====================
    # Define the pipeline flow
    fetch_reviews >> validate_reviews >> prepare_ray >> ray_processing >> store_results >> generate_summary >> notify
    
 