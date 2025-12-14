import psycopg2
import pandas as pd
import os
from datetime import datetime

conn = psycopg2.connect(host='postgres', port=5432, database='airflow', user='airflow', password='airflow')
df = pd.read_sql_query('SELECT * FROM reviews ORDER BY processed_timestamp DESC', conn)
os.makedirs('/opt/airflow/outputs/csv', exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
output_file = f'/opt/airflow/outputs/csv/reviews_export_{timestamp}.csv'
df.to_csv(output_file, index=False)
print(f'Exported {len(df)} reviews to {output_file}')

# Summary
summary_df = df.groupby('sentiment_label').agg({
    'review_id': 'count',
    'sentiment_score': 'mean',
    'rating': 'mean'
}).round(3)
summary_df.columns = ['count', 'avg_score', 'avg_rating']
summary_file = f'/opt/airflow/outputs/csv/sentiment_summary_{timestamp}.csv'
summary_df.to_csv(summary_file)
print(f'Exported summary to {summary_file}')
print('\nSummary:')
print(summary_df)

conn.close()
