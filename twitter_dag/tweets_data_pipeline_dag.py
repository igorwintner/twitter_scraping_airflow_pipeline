from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from twitter_etl.twitter_etl import extract_tweets, load_to_s3

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
dag = DAG(
    'tweets_data_pipeline',
    default_args=default_args,
    description='A simple DAG for scraping tweets and uploading to S3',
    schedule_interval=None, 
    start_date=datetime.now(),
    catchup=False,
)

# Define the tasks
t1 = PythonOperator(
    task_id='extract_tweets',
    python_callable=extract_tweets,
    dag=dag,
)

t2 = PythonOperator(
    task_id='load_to_s3',
    python_callable=lambda: load_to_s3(bucket_name='airflow-tweets-project-bucket'),
    dag=dag,
)

# Define the task dependencies
t1 >> t2