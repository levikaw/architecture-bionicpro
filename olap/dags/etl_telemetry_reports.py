from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import csv
from dotenv import load_dotenv
import os

load_dotenv()

CLICKHOUSE_USERNAME = os.getenv("CLICKHOUSE_USERNAME")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE")

DATABASE_URI = f"clickhouse://{CLICKHOUSE_USERNAME}:{CLICKHOUSE_PASSWORD}@olap_db:8123/{CLICKHOUSE_DATABASE}"
# DATABASE_URI = f"clickhouse://airflow:airflow@olap_db:8123/airflow"

def extract_telemetry_data():
    CSV_FILE_PATH = 'sample_files/sample.csv'
    with open( CSV_FILE_PATH, 'r') as csvfile:
        csvreader = csv.reader(csvfile)

        insert_queries = []
        is_header = True
        for row in csvreader:
            if is_header:
                is_header = False
                continue
            insert_query = f"INSERT INTO airflow.reports (user_email,prosthesis_id,signal_strength,battery_percentage) VALUES ('{row[0]}', '{row[1]}', {row[2]},{row[3]});"
            insert_queries.append(insert_query)
        
        with open('./dags/sql/insert_queries.sql', 'w') as f:
            for query in insert_queries:
                f.write(f"{query}\n")

def create_report_table():
    engine = create_engine(DATABASE_URI)
    with engine.begin() as conn:
        conn.execute(text("""CREATE TABLE IF NOT EXISTS airflow.reports (
    user_email String,
    prosthesis_id String,
    signal_strength Float32,
    battery_percentage Int32,
) 
ENGINE = MergeTree()
ORDER BY (user_email);"""))
        
def load_data():
    engine = create_engine(DATABASE_URI)
    with engine.begin() as conn:
        with open('./dags/sql/insert_queries.sql', 'r') as f:
            conn.execute(text(f.read()))


with DAG(
    'etl_telemetry_reports',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2023, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
    },
    description='ETL process to create telemetry reports',
    schedule_interval='@daily',
) as dag:

    extract = PythonOperator(
        task_id='extract_telemetry_data',
        python_callable=extract_telemetry_data
    )

    create_table = PythonOperator(
        task_id='create_report_table',
        python_callable=create_report_table,
    )

    load = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    create_table >> extract >> load
