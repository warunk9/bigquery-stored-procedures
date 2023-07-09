import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


dag = DAG(
    dag_id='BIXFM_ROY_LOAD_REV_DATAFILE',
    description='A DAG that reads Python from GCS and runs',
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
)

BISH_ROY_REV_CP= BigQueryExecuteQueryOperator(
    task_id='BISH_ROY_REV_CP', 
    sql='ssh_exec.sh "localhost","cp ${EDW_HOME}/partners/bi_finance/newroot/incoming/RevEXPIntl_{#1}.csv ${EDW_HOME}/landing/R2D2_WRK/monthly/{#2}/R2D2_WRK-NAPSTER_REV_FILE-monthly-{#2}.dat;"', 
    use_legacy_sql=False,
    location="US",
    dag=dag)

BITFL_R2D2_WRK_NAPSTER_REVENUE= BigQueryExecuteQueryOperator(
    task_id='BITFL_R2D2_WRK_NAPSTER_REVENUE', 
    sql='fileloader.sh', 
    use_legacy_sql=False,
    location="US",
    dag=dag)

                                                         
end_task = BashOperator(
    task_id='end_task',
    bash_command='echo "job runs successfully" ',
    dag=dag)



BISH_ROY_REV_CP >> BITFL_R2D2_WRK_NAPSTER_REVENUE >> end_task