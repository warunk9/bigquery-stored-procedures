import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


dag = DAG(
    dag_id='BIREP_PUB_MONTHLY',
    description='A DAG that reads Python from GCS and runs',
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
)

BIFG_RG_EFFECTIVE_RATE= BigQueryExecuteQueryOperator(
    task_id='BIFG_RG_EFFECTIVE_RATE', sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)
BIFG_RG_EFFECTIVE_ROYALTY_RATE= BigQueryExecuteQueryOperator(
    task_id='BIFG_RG_EFFECTIVE_ROYALTY_RATE',
    sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)
                                                       
end_task = BashOperator(
    task_id='end_task',
    bash_command='echo "job runs successfully" ',
    dag=dag)


BIFG_RG_EFFECTIVE_RATE >> BIFG_RG_EFFECTIVE_ROYALTY_RATE >> end_task
