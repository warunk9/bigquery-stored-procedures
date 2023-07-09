import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


dag = DAG(
    dag_id='BIXFM_ROY_PUB_CLOSE_MONTH',
    description='A DAG that reads Python from GCS and runs',
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
)

BITSP_ROY_CALC_GROSS_REV_MONTH= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_CALC_GROSS_REV_MONTH', 
    sql='CALL `CALC_BUCKET_GROSS_REVENUE_xfm;`',   
    use_legacy_sql=False,location="US",
    dag=dag)

BITSP_ROY_CALC_DEDUCTION_MONTH= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_CALC_DEDUCTION_MONTH', 
    sql='CALL `DEDUCTION_CALC_MONTH_0_xfm;`',   
    use_legacy_sql=False,location="US",
    dag=dag)

BITSP_ROY_COUNT_SUB_MONTH= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_COUNT_SUB_MONTH', 
    sql='CALL `COUNT_SUB_MONTH_0_xfm;`',   
    use_legacy_sql=False,location="US",
    dag=dag)

BITSP_ROY_PUB_USAGE_MONTH= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_PUB_USAGE_MONTH', 
    sql='CALL `PUBLISHING_USAGE_MONTH_0_xfm;`',   
    use_legacy_sql=False,location="US",
    dag=dag)

BITSP_ROY_GROSS_REV_ADJ_MONTH= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_GROSS_REV_ADJ_MONTH', 
    sql='CALL `BUCKET_GROSS_REVENUE_ALLOCATE_;`',   
    use_legacy_sql=False,location="US",
    dag=dag)

BITSP_ROY_PUB_ACCRUAL_MONTH= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_PUB_ACCRUAL_MONTH', 
    sql='CALL `PUB_ACCRUAL_MONTH_F_0_xfm;`',   
    use_legacy_sql=False,location="US",
    dag=dag)

BITSP_PUB_CONTENT_SW_WRK= BigQueryExecuteQueryOperator(
    task_id='BITSP_PUB_CONTENT_SW_WRK', 
    sql='CALL `PUB_CONTENT_SW_WRK_0_xfm;`',   
    use_legacy_sql=False,location="US",
    dag=dag)

BITSP_ROY_US_SESAC_CALC= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_US_SESAC_CALC', 
    sql='CALL `PRO_SESAC_CALC_xfm;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag)


                                                         
end_task = BashOperator(
    task_id='end_task',
    bash_command='echo "job runs successfully" ',
    dag=dag)



BITSP_ROY_CALC_GROSS_REV_MONTH >> [BITSP_ROY_CALC_DEDUCTION_MONTH,BIXFM_ROY_BOUNTY_MONTHLY] >> BITSP_ROY_COUNT_SUB_MONTH >> BITSP_ROY_PUB_USAGE_MONTH >> BITSP_ROY_GROSS_REV_ADJ_MONTH >> BITSP_ROY_PUB_ACCRUAL_MONTH >> BITSP_PUB_CONTENT_SW_WRK >> BITSP_ROY_US_SESAC_CALC >> end_task
