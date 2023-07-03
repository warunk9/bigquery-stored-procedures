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

BITSP_ROY_ERROR_RULES = BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_ERROR_RULES',
    sql='CALL `stored_procs.roy_error_rules_0_xfm` ("202301");',
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BITSP_ROY_NEW_ROYALTY_JOB_01 = BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_NEW_ROYALTY_JOB_01',
    sql='CALL `stored_procs.insert_royalty_job_0_xfm` ("202301",1);',
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BITSP_ROY_NEW_ROYALTY_JOB = BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_NEW_ROYALTY_JOB',
    sql='CALL `stored_procs.insert_royalty_job_0_xfm` ("202301",1);',
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BITSP_ROY_BUCKET_GROUP_MAP = BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_BUCKET_GROUP_MAP',
    sql='CALL `stored_procs.roy_bucket_group_map_0_xfm` ("202301");',
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BITSP_ROY_BUCKET_COUNTRY_MV = BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_BUCKET_COUNTRY_MV',
    sql='CALL `stored_procs.entity_bucket_country_mv_0_xfm`;',
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BITSP_ROY_PARTNER_EMPLOYEE = BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_PARTNER_EMPLOYEE',
    sql='CALL `stored_procs.load_partner_employee_0_xfm` ("202301",2);',
    use_legacy_sql=False,
    location="US",
    dag=dag
)

end_task = BashOperator(
    task_id='end_task',
    bash_command='echo "job runs successfully" ',
    dag=dag)

BITSP_ROY_ERROR_RULES >> BITSP_ROY_NEW_ROYALTY_JOB_01 >> BITSP_ROY_NEW_ROYALTY_JOB >> BITSP_ROY_BUCKET_GROUP_MAP >> BITSP_ROY_BUCKET_COUNTRY_MV >> BITSP_ROY_PARTNER_EMPLOYEE >>  end_task
