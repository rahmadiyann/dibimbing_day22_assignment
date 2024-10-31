from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "rian",
    "retry_delay": timedelta(minutes=5),
    "retries": 3,
}

dag = DAG(
    dag_id="olist_analysis_pipeline",
    default_args=default_args,
    schedule_interval="0 0 * * *",  # Run daily
    dagrun_timeout=timedelta(minutes=60),
    description="Olist customer analysis pipeline",
    start_date=days_ago(1),
    catchup=False,
)

# Dummies
start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag, trigger_rule="all_success")

# ETL Task
etl_task = SparkSubmitOperator(
    application="/spark-scripts/olist_etl_load.py",
    conn_id="spark_main",
    task_id="olist_etl_load",
    jars="/spark-scripts/jars/jars_postgresql-42.2.20.jar",
    dag=dag,
)

# Analysis Task
analysis_task = SparkSubmitOperator(
    application="/spark-scripts/olist_analysis.py",
    conn_id="spark_main",
    task_id="olist_analysis",
    jars="/spark-scripts/jars/jars_postgresql-42.2.20.jar",
    dag=dag,
)

# Set task dependencies
start >> etl_task >> analysis_task >> end
