from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

import sys
sys.path.append('/opt/airflow/modules')
from bunjang_crawler import collect_and_filter_data, merge_results

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bunjang_crawler',
    default_args=default_args,
    description='Bunjang crawler DAG',
    schedule_interval=timedelta(days=1),
)

def crawl_and_filter_brand(brand, **kwargs):
    output_file = f"/opt/airflow/output/{brand[0]}_products.json"
    collect_and_filter_data(brand, output_file)

def merge_results_task(**kwargs):
    input_dir = "/opt/airflow/output"
    output_file = "/opt/airflow/output/all_products.json"
    merge_results(input_dir, output_file)

with open("/opt/airflow/data/brands.json", "r", encoding="utf-8") as file:
    brand_names = json.load(file)

crawl_tasks = []
for brand in brand_names.items():
    task = PythonOperator(
        task_id=f"crawl_and_filter_{brand[0]}",
        python_callable=crawl_and_filter_brand,
        op_kwargs={"brand": brand},
        dag=dag,
    )
    crawl_tasks.append(task)

merge_task = PythonOperator(
    task_id="merge_results",
    python_callable=merge_results_task,
    dag=dag,
)

crawl_tasks >> merge_task