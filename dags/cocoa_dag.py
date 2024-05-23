from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.operators.bash import BashOperator


from cocoa_scripts.cocoa_get_data import get_cocoa_data_and_save_to_qdb
from cocoa_scripts.cocoa_send_data import get_cocoa_data_and_send_to_gsheet

default_args = {
    'owner': 'admin',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    default_args=default_args,
    dag_id='cocoa_data_v44',
    start_date=datetime(2024,5,12),
    schedule='5 4 * * 2-6'
) as dag:

    task1 = PythonVirtualenvOperator(
        task_id="get_cocoa_data",
        python_callable=get_cocoa_data_and_save_to_qdb,
        requirements=["pandas", "openpyexcel", "requests", "pyexcel", "pyexcel-xlsx",
                      "pyexcel-xls", "numpy", "questdb", "pyarrow"]
    )

    task2 = PythonVirtualenvOperator(
        task_id="send_data_to_gsheet",
        python_callable=get_cocoa_data_and_send_to_gsheet,
        requirements=["pandas", "gspread", "oauth2client", "PyOpenSSL"]
    )

    # task2 = PythonOperator(
    #     task_id="get_cocoa_data",
    #     python_callable=cocoa_script.get_cocoa_data_and_save_to_qdb,
    # )

    task1 >> task2