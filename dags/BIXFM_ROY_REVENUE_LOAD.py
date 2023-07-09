import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


dag = DAG(
    dag_id='BIXFM_ROY_REVENUE_LOAD',
    description='A DAG that reads Python from GCS and runs',
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
)

end_task = BashOperator(
    task_id='end_task',
    bash_command='echo "job runs successfully" ',
    dag=dag)


end_task

