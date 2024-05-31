from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from report_generate.main import generate_weekly_winners_losers
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'admin',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    default_args=default_args,
    dag_id='report_dag_stk_v8',
    start_date=datetime(2024,5,25),
    schedule='30 7 * * 1'
) as dag:
    task1 = PythonVirtualenvOperator(
        task_id="send_email_for_weekly_stk_change",
        python_callable=generate_weekly_winners_losers,
        requirements=["pandas", "plotly", "requests", "jinja2", "kaleido==0.0.3.post1"]
    )