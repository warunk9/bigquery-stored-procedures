import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


dag = DAG(
    dag_id='BIXFM_ROY_MONTHLY',
    description='A DAG that reads Python from GCS and runs',
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
)

BITSP_ROY_NEW_ROYALTY_JOB= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_NEW_ROYALTY_JOB', 
    sql='CALL `INSERT_ROYALTY_JOB_0_xfm;`',   
    use_legacy_sql=False,location="US",dag=dag)

BITSP_DM_BOUNTY_SUB_MONTH= BigQueryExecuteQueryOperator(
    task_id='BITSP_DM_BOUNTY_SUB_MONTH', 
    sql='CALL `DM_BOUNTY_SUB_MONTH_0_xfm;`',   
    use_legacy_sql=False,location="US",dag=dag)

BITSP_DM_BOUNTY_PER_SUB_CALC= BigQueryExecuteQueryOperator(
    task_id='BITSP_DM_BOUNTY_PER_SUB_CALC', 
    sql='CALL `DM_BOUNTY_PER_SUB_CALC_0_xfm;`',   
    use_legacy_sql=False,location="US",
    dag=dag
    )

BITSP_ROY_BNTY_GROSS_REV_MONTH= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_BNTY_GROSS_REV_MONTH', 
    sql='CALL `BOUNTIES_REVENUE_MONTH_0_xfm;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BITSP_DM_DIST_REV_SHARE_CALC= BigQueryExecuteQueryOperator(
    task_id='BITSP_DM_DIST_REV_SHARE_CALC', 
    sql='CALL `DM_DIST_REV_SHARE_CALC_0_xfm;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BITSP_DM_GL_REV_SUB_ATTRIBUTE= BigQueryExecuteQueryOperator(
    task_id='BITSP_DM_GL_REV_SUB_ATTRIBUTE', 
    sql='CALL `GL_REVENUE_SUB_ATTRIBUTE_xfm;`',  
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BITSP_DM_PRES_DIST_CALC_FINAL= BigQueryExecuteQueryOperator(
    task_id='BITSP_DM_PRES_DIST_CALC_FINAL', 
    sql='CALL `DM_PRES_DIST_CALC_FINAL_0_xfm;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BITSP_GL_FILE= BigQueryExecuteQueryOperator(
    task_id='BITSP_GL_FILE', 
    sql='CALL ` ;`',   
    use_legacy_sql=False,location="US",
    dag=dag
)

BIREP_RG_PARTNER_PLAY_FILE= BigQueryExecuteQueryOperator(
    task_id='BIREP_RG_PARTNER_PLAY_FILE', 
    sql='SamsungPlays.tpt',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BIREP_RG_PARTNER_USER_FILE= BigQueryExecuteQueryOperator(
    task_id='BIREP_RG_PARTNER_USER_FILE', sql='CALL `;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag
)

                                                         
end_task = BashOperator(
    task_id='end_task',
    bash_command='echo "job runs successfully" ',
    dag=dag)



BITSP_ROY_NEW_ROYALTY_JOB  >> BITSP_DM_BOUNTY_SUB_MONTH >> BITSP_DM_BOUNTY_PER_SUB_CALC >> [BITSP_DM_PRES_DIST_CALC_FINAL,BITSP_ROY_BNTY_GROSS_REV_MONTH] >> BITSP_DM_DIST_REV_SHARE_CALC >> [BITSP_DM_PRES_DIST_CALC_FINAL , BITSP_DM_GL_REV_SUB_ATTRIBUTE] >> BITSP_GL_FILE >> BIREP_RG_PARTNER_PLAY_FILE >> BIREP_RG_PARTNER_USER_FILE >> end_task