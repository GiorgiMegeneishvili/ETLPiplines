from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from etls import ExtractDataCoinGeko as w

with DAG(
    dag_id="pipeline_ETL_Coin_Data",
    start_date=datetime(2026, 1, 1),
    schedule="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
) as dag:
    PythonOperator(
        task_id="extract_Coin_data",
        python_callable=w.start,
    )
