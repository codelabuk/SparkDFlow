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
    conf={
      "spark.eventLog.enabled": "true",
      "spark.eventLog.dir": "/opt/spark/logs"
    },
    dag=dag
)

scala_job = SparkSubmitOperator(
    task_id="scala_job",
    conn_id="spark-conn",
    application="jobs/scala/target/scala-2.12/word-count_2.12-0.1.jar",
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs complete successfully"),
    dag=dag
)

start_task >> [python_job, scala_job] >> end