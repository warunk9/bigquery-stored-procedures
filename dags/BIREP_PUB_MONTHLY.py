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

BIFG_PUB_MARKET_SHARE= BigQueryExecuteQueryOperator(
    task_id='BIFG_PUB_MARKET_SHARE',
    sql='CALL `;`',
    use_legacy_sql=False,
    location="US",dag=dag
)

BIFD_PUB_MARKET_SHARE= BigQueryExecuteQueryOperator(
    task_id='BIFD_PUB_MARKET_SHARE', 
    sql='CALL `;`',
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BIFG_SESAC= BigQueryExecuteQueryOperator(
    task_id='BIFG_SESAC', sql='CALL `;`',
    use_legacy_sql=False,location="US",
    dag=dag
    )

BIFD_SESAC= BigQueryExecuteQueryOperator(
    task_id='BIFD_SESAC', sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BIFG_PUB_ACCRUAL= BigQueryExecuteQueryOperator(
    task_id='BIFG_PUB_ACCRUAL', 
    sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",dag=dag
)

BIFD_PUB_ACCRUAL= BigQueryExecuteQueryOperator(
    task_id='BIFD_PUB_ACCRUAL', sql='CALL `;`',   
    use_legacy_sql=False,location="US",dag=dag
)

BIFG_PUB_REV_SUB= BigQueryExecuteQueryOperator(
    task_id='BIFG_PUB_REV_SUB', 
    sql='CALL `;`',  
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BIFD_PUB_REV_SUB= BigQueryExecuteQueryOperator(
    task_id='BIFD_PUB_REV_SUB', 
    sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BITSP_ROY_US_PRO_SUMMARY= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_US_PRO_SUMMARY', 
    sql='CALL `PRO_US_CALC_SUMMARY_0_xfm;`',   
    use_legacy_sql=False,location="US",
    dag=dag
)
BITSP_ROY_US_MECH_CALC= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_US_MECH_CALC', 
    sql='CALL `MECHANICAL_CALC_xfm;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

                                                       
end_task = BashOperator(
    task_id='end_task',
    bash_command='echo "job runs successfully" ',
    dag=dag
)



BIFG_PUB_MARKET_SHARE >> BIFD_PUB_MARKET_SHARE >> BIFG_SESAC >> BIFD_SESAC >> BIFG_PUB_ACCRUAL >> BIFD_PUB_ACCRUAL >> BIFG_PUB_REV_SUB >> BIFD_PUB_REV_SUB >> BITSP_ROY_US_PRO_SUMMARY >> BITSP_ROY_US_MECH_CALC >> end_task



