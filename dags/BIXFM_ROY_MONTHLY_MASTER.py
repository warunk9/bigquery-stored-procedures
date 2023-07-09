import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


dag = DAG(
    dag_id='BIXFM_ROY_MONTHLY_MASTER',
    description='A DAG that reads Python from GCS and runs',
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
)

STAT_USER_EXCEPTION_DAY_F= BigQueryExecuteQueryOperator(
    task_id='STAT_USER_EXCEPTION_DAY_F', 
    sql='td_table_stats.sh "ROYALTY_DM", "USER_EXCEPTION_DAY_F"', 
    use_legacy_sql=False,
    location="US",
    dag=dag)

STAT_BUCKET_PLAY_DAY_F= BigQueryExecuteQueryOperator(
    task_id='STAT_BUCKET_PLAY_DAY_F', 
    sql='td_table_stats.sh` "ROYALTY_DM", "BUCKET_PLAY_COUNT_DAY_F"', 
    use_legacy_sql=False,
    location="US",
    dag=dag)

STAT_DETAIL_STREAM_F= BigQueryExecuteQueryOperator(
    task_id='STAT_DETAIL_STREAM_F', 
    sql='td_table_stats.sh "ROYALTY_DM", "BUCKET_DETAIL_STREAM_F"', 
    use_legacy_sql=False,location="US",
    dag=dag)
                                                         
end_task = BashOperator(
    task_id='end_task',
    bash_command='echo "job runs successfully" ',
    dag=dag)


STAT_USER_EXCEPTION_DAY_F >> [STAT_BUCKET_PLAY_DAY_F , STAT_DETAIL_STREAM_F] >> end_task

