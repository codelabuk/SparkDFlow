from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id='spark_airflow',
    default_args = {
        'owner': 'Afzal Ahmed',
        'start_date':  datetime(2026, 3, 31),
    },
    schedule="@daily"
)

start_task = PythonOperator(
    task_id='start_task',
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

python_job = SparkSubmitOperator(
    task_id="python_job",
    conn_id="spark-conn",
    application="jobs/python/word_count_job.py",
    dag=dag
)
end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs ended"),
    dag=dag
)

start_task >> python_job >> end