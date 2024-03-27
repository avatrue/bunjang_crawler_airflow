from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pytz import timezone
import json
import requests
import sys

sys.path.append('/opt/airflow/modules')
from bunjang_crawler import collect_and_filter_data, merge_results

KST = timezone('Asia/Seoul')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 27, 12, 0, tzinfo=KST),  # 한국 시간대로 시작 날짜와 시간을 설정
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bunjang_crawler_test',
    default_args=default_args,
    description='Bunjang crawler DAG test',
    schedule_interval='0 12 * * *',  # 한국 시간 기준 매일 낮 12시에 실행되도록 cron 표현식 사용
    catchup=False,  # 과거의 실행을 생략하고 현재 시점부터 실행
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