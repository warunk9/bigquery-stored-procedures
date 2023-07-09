import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


dag = DAG(
    dag_id='BIREP_RG_INTERNAL_STMT_GEN_CSV',
    description='A DAG that reads Python from GCS and runs',
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
)

BIFG_UMG_S_BR_MONTHEND= BigQueryExecuteQueryOperator(
    task_id='BIFG_UMG_S_BR_MONTHEND', 
    sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BIFG_UMG_S_EURO_MONTHEND= BigQueryExecuteQueryOperator(
    task_id='BIFG_UMG_S_EURO_MONTHEND', 
    sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BIFG_UMG_S_LATAM_MONTHEND= BigQueryExecuteQueryOperator(
    task_id='BIFG_UMG_S_LATAM_MONTHEND', 
    sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BIFG_UMG_S_US_MONTHEND= BigQueryExecuteQueryOperator(
    task_id='BIFG_UMG_S_US_MONTHEND', 
    sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BIFG_UMG_S_CA_MONTHEND= BigQueryExecuteQueryOperator(
    task_id='BIFG_UMG_S_CA_MONTHEND', 
    sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BIFG_SONY_S_HW_EU_MONTHEND= BigQueryExecuteQueryOperator(
    task_id='BIFG_SONY_S_HW_EU_MONTHEND', 
    sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BIFG_SONY_S_HW_LA_MONTHEND= BigQueryExecuteQueryOperator(
    task_id='BIFG_SONY_S_HW_LA_MONTHEND', 
    sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BIFG_SONY_S_BR_MONTHEND= BigQueryExecuteQueryOperator(
    task_id='BIFG_SONY_S_BR_MONTHEND', 
    sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BIFG_SONY_S_EURO_MONTHEND= BigQueryExecuteQueryOperator(
    task_id='BIFG_SONY_S_EURO_MONTHEND', 
    sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BIFG_SONY_S_LATAM_MONTHEND= BigQueryExecuteQueryOperator(
    task_id='BIFG_SONY_S_LATAM_MONTHEND', 
    sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BIFG_KONTOR_STMT_MONTHEND= BigQueryExecuteQueryOperator(
    task_id='BIFG_KONTOR_STMT_MONTHEND', 
    sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BIFG_SONY_S_US_MONTHEND= BigQueryExecuteQueryOperator(
    task_id='BIFG_SONY_S_US_MONTHEND', 
    sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BIFD_MUSICSALES_SOUNDEX_ACRL= BigQueryExecuteQueryOperator(
    task_id='BIFD_MUSICSALES_SOUNDEX_ACRL', 
    sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BIREP_RG_US_PRO_CALC_REPORT= BigQueryExecuteQueryOperator(
    task_id='BIREP_RG_US_PRO_CALC_REPORT', 
    sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BIREP_RG_BOUNTIES_GL_FILE= BigQueryExecuteQueryOperator(
    task_id='BIREP_RG_BOUNTIES_GL_FILE', 
    sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

                                                         
end_task = BashOperator(
    task_id='end_task',
    bash_command='echo "job runs successfully" ',
    dag=dag)

BIFG_UMG_S_BR_MONTHEND >> BIFG_UMG_S_EURO_MONTHEND >> BIFG_UMG_S_LATAM_MONTHEND >> BIFG_UMG_S_US_MONTHEND >> BIFG_UMG_S_CA_MONTHEND >> BIFG_SONY_S_HW_EU_MONTHEND >> BIFG_SONY_S_HW_LA_MONTHEND >> BIFG_SONY_S_BR_MONTHEND >> BIFG_SONY_S_EURO_MONTHEND >> BIFG_SONY_S_LATAM_MONTHEND >> [BIFG_KONTOR_STMT_MONTHEND , BIFG_SONY_S_US_MONTHEND] >> BIFD_MUSICSALES_SOUNDEX_ACRL >> BIREP_RG_US_PRO_CALC_REPORT >> [BIREP_RG_BOUNTIES_GL_FILE,BIREP_RG_EFFECTIVE_ROYALTY_RAT] >> end_task