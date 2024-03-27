from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from scrapers.stock_data_scraper import StockDataScraper
import os
from dotenv import load_dotenv

load_dotenv()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 25),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'stock_data_scraper',
    default_args=default_args,
    description='Scrape stock data and load it into BigQuery',
    schedule_interval='@daily',
)

def scrape_and_load_data():
    project_id = os.environ["PROJECT_ID"]
    dataset_id = os.environ['DATASET_ID']
    table_id = os.environ['TABLE_ID']
    
    scraper = StockDataScraper(project_id, dataset_id, table_id)
    scraper.fetch_data()
    scraper.create_bigquery_table()
    scraper.process_data()
    scraper.load_data_to_bigquery()

with dag:
    scrape_task = PythonOperator(
        task_id='scrape_and_load_data',
        python_callable=scrape_and_load_data,
    )