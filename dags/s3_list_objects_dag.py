from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator

def _branch_func(ti=None):
    """python branch operator function to pull xcom pushed by previous list_objects_task
        to check list S3 bucket and validate if file exists, if file doesn't exist then 
        return the task name notify vendor
    """
    xcom_value = ti.xcom_pull(task_ids="list_objects_task")
    print(f"received xcom_value {xcom_value}")
    if xcom_value:
        return "emr_process_file"
    else:
        return "notify_vendor"


def _notify_vendor():
    """TODO: implement the email or slack function to notify vendor about the filem misssing"""
    print("sales missing")

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "end_date": datetime(2024, 1, 2),
    "email_on_failure": False,
    "email_on_retry": False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    "s3_list_objects_dag",
    default_args=default_args,
    description="DAG to list objects in an S3 bucket",
    catchup=True,
    schedule_interval="0 6 * * *",
)


s3_bucket = "wcddeb8-lab-airflow-zheng"
# s3_prefix = "your-s3-prefix"


list_objects_task = S3ListOperator(
    task_id="list_objects_task",
    bucket=s3_bucket,
    prefix="{{ macros.ds_format(ds, '%Y-%m-%d', '%Y/%m/%d') }}",
    delimiter="",  # Optional delimiter to use when listing objects
    # aws_conn_id="aws_default",
    dag=dag,
)

check_s3_file_exist=BranchPythonOperator(
    task_id="check_s3_file_exist",
    python_callable=_branch_func,
    dag=dag
)

notify_vendor = PythonOperator(
    task_id="notify_vendor",
    python_callable=_notify_vendor,
    dag=dag
)

emr_process_file = BashOperator(
    task_id="emr_process_file",
    bash_command="echo processing files",
    dag=dag
)

(
    list_objects_task
    >> check_s3_file_exist
    >> [notify_vendor,emr_process_file]
)

