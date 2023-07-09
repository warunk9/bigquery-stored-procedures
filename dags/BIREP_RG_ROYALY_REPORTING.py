import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


dag = DAG(
    dag_id='BIREP_RG_ROYALY_REPORTING',
    description='A DAG that reads Python from GCS and runs',
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
)

BIREP_RG_GL_LABEL_ROY_FILE= BigQueryExecuteQueryOperator(
    task_id='BIREP_RG_GL_LABEL_ROY_FILE', 
    sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)
BIREP_RG_AP_LABEL_ROY_FILE= BigQueryExecuteQueryOperator(
    task_id='BIREP_RG_AP_LABEL_ROY_FILE', 
    sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)
BIREP_RG_US_PRO_GL_FILE= BigQueryExecuteQueryOperator(
    task_id='BIREP_RG_US_PRO_GL_FILE', sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)
BIREP_RG_US_MECH_ANALYSIS_FILE= BigQueryExecuteQueryOperator(
    task_id='BIREP_RG_US_MECH_ANALYSIS_FILE', sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)
BIREP_RG_US_MECH_GL_FILE= BigQueryExecuteQueryOperator(
    task_id='BIREP_RG_US_MECH_GL_FILE', sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)
                                                         
end_task = BashOperator(
    task_id='end_task',
    bash_command='echo "job runs successfully" ',
    dag=dag)

BIREP_RG_GL_LABEL_ROY_FILE >> [BIREP_RG_AP_LABEL_ROY_FILE , BIREP_RG_US_PRO_GL_FILE] >> BIREP_RG_US_MECH_ANALYSIS_FILE >> BIREP_RG_US_MECH_GL_FILE >> end_task
