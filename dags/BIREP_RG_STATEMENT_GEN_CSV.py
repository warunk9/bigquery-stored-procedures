import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


dag = DAG(
    dag_id='BIREP_RG_STATEMENT_GEN_CSV',
    description='A DAG that reads Python from GCS and runs',
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
)

BIFG_UMG_S_WW_MONTHEND= BigQueryExecuteQueryOperator(
    task_id='BIFG_UMG_S_WW_MONTHEND', sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BIFG_UMG_S_US_MONTHEND= BigQueryExecuteQueryOperator(
    task_id='BIFG_UMG_S_US_MONTHEND', sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BIFG_WMG_S_CA_MONTEND= BigQueryExecuteQueryOperator(
    task_id='BIFG_WMG_S_CA_MONTEND', 
    sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BIFG_WMG_S_BR_MONTHEND= BigQueryExecuteQueryOperator(
    task_id='BIFG_WMG_S_BR_MONTHEND', 
    sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BIFG_WMG_S_EURO_MONTHEND= BigQueryExecuteQueryOperator(
    task_id='BIFG_WMG_S_EURO_MONTHEND', sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BIFG_WMG_S_LATAM_MONTHEND= BigQueryExecuteQueryOperator(
    task_id='BIFG_WMG_S_LATAM_MONTHEND', 
    sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BIFG_WMG_S_US_MONTHEND= BigQueryExecuteQueryOperator(
    task_id='BIFG_WMG_S_US_MONTHEND', 
    sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)
BIFG_SONY_S_CA_MONTHEND= BigQueryExecuteQueryOperator(
    task_id='BIFG_SONY_S_CA_MONTHEND', 
    sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

                                                         
end_task = BashOperator(
    task_id='end_task',
    bash_command='echo "job runs successfully" ',
    dag=dag)

BIFG_UMG_S_WW_MONTHEND >> BIFG_UMG_S_US_MONTHEND >> BIFG_WMG_S_CA_MONTEND >> BIFG_WMG_S_BR_MONTHEND >> BIFG_WMG_S_EURO_MONTHEND >> BIFG_WMG_S_LATAM_MONTHEND >> BIFG_WMG_S_US_MONTHEND >> BIFG_SONY_S_CA_MONTHEND >> BIREP_RG_INTERNAL_STMT_GEN_CSV >> end_task