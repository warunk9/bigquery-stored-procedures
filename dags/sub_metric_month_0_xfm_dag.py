import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


dag = DAG(
    dag_id='calc_bucket_sub_0_xfm_dag_1',
    description='A DAG that reads Python from GCS and runs',
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
)

call_stored_procedure = BigQueryExecuteQueryOperator(
    task_id='call_stored_procedure',
    sql='CALL `stored_procs.sub_metric_month_0_xfm` ("202301");',
    use_legacy_sql=False,
    location="US",
    dag=dag
)

end_task = BashOperator(
    task_id='end_task',
    bash_command='echo "job runs successfully" ',
    dag=dag)

call_stored_procedure >> end_task
