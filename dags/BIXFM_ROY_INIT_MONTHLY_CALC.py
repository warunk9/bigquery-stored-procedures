import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


dag = DAG(
    dag_id='BIXFM_ROY_INIT_MONTHLY_CALC',
    description='A DAG that reads Python from GCS and runs',
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
)
BITSP_ROY_ERROR_RULES= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_ERROR_RULES', 
    sql='CALL `ROY_ERROR_RULES_0_xfm("202303");`', 
    use_legacy_sql=False,location="US",
    dag=dag)

BITSP_ROY_NEW_ROYALTY_JOB_01= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_NEW_ROYALTY_JOB_01', 
    sql='CALL `INSERT_ROYALTY_JOB_0_xfm("202303",1);`', 
    use_legacy_sql=False,location="US",
    dag=dag)

BITSP_ROY_NEW_ROYALTY_JOB= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_NEW_ROYALTY_JOB', 
    sql='CALL `INSERT_ROYALTY_JOB_0_xfm("202303",3);`', 
    use_legacy_sql=False,location="US",
    dag=dag)

BITSP_ROY_BUCKET_GROUP_MAP= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_BUCKET_GROUP_MAP', 
    sql='CALL `ROY_BUCKET_GROUP_MAP_0_xfm;`', 
    use_legacy_sql=False,
    location="US",dag=dag)

BITSP_ROY_BUCKET_COUNTRY_MV= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_BUCKET_COUNTRY_MV', 
    sql='CALL `ENTITY_BUCKET_COUNTRY_MV_0_xfm;`', 
    use_legacy_sql=False,location="US",dag=dag)

BITSP_ROY_PARTNER_EMPLOYEE= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_PARTNER_EMPLOYEE', 
    sql='CALL `LOAD_PARTNER_EMPLOYEE_0_xfm("202303",2);`', 
    use_legacy_sql=False,location="US",
    dag=dag)

end_task = BashOperator(
    task_id='end_task',
    bash_command='echo "job runs successfully" ',
    dag=dag)

BITSP_ROY_ERROR_RULES >> BITSP_ROY_NEW_ROYALTY_JOB_01 >> BITSP_ROY_NEW_ROYALTY_JOB >> BITSP_ROY_BUCKET_GROUP_MAP >> BITSP_ROY_BUCKET_COUNTRY_MV >> BITSP_ROY_PARTNER_EMPLOYEE >> end_task
