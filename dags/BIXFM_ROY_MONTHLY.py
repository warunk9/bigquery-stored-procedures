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

BITSP_ROY_BUCKET_PROP_SHARE= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_BUCKET_PROP_SHARE', 
    sql='CALL `BUCKET_PROP_SHARE_MONTH_xfm;`',   
    use_legacy_sql=False,location="US",
    dag=dag)

BITSP_ROY_CALC_PER_PLAY_MONTH= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_CALC_PER_PLAY_MONTH', 
    sql='CALL `CALC_PER_PLAY_xfm;`',   
    use_legacy_sql=False,location="US",
    dag=dag)

BITSP_ROY_COUNT_SUB_MONTH= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_COUNT_SUB_MONTH', 
    sql='CALL `COUNT_SUB_MONTH_0_xfm;`',   
    use_legacy_sql=False,location="US",
    dag=dag)

BITSP_ROY_CALC_GROSS_REV_MONTH= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_CALC_GROSS_REV_MONTH', 
    sql='CALL `CALC_BUCKET_GROSS_REVENUE_xfm;`',   
    use_legacy_sql=False,location="US",
    dag=dag)

BISTP_ROY_UPCHARGE_SUB_MONTH= BigQueryExecuteQueryOperator(
    task_id='BISTP_ROY_UPCHARGE_SUB_MONTH', 
    sql='CALL `UPCHARGE_COUNT_SUB_MONTH_0_xfm;`',   
    use_legacy_sql=False,location="US",
    dag=dag)

BITSP_ROY_CALC_DEDUCTION_MONTH= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_CALC_DEDUCTION_MONTH', 
    sql='CALL `DEDUCTION_CALC_MONTH_0_xfm;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag)

BITSP_ROY_ESCALATION_BLENDER= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_ESCALATION_BLENDER', 
    sql='CALL `ESCALATION_BLENDER_0_xfm;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag)

BITSP_ROY_PSM_RATE_FINDER= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_PSM_RATE_FINDER', 
    sql='CALL `PSM_RATE_FINDER_0_xfm;`',   
    use_legacy_sql=False,
    location="US",
    dag=dag)



BITSP_ROY_REV_RATE_FINDER= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_REV_RATE_FINDER', 
    sql='CALL `REV_RATE_FINDER_0_xfm;`',   
    use_legacy_sql=False,location="US",
    dag=dag)

BITSP_ROY_UPCHARGE_PSM_RATE= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_UPCHARGE_PSM_RATE', 
    sql='CALL `UMG_UPCHARGE_PSM_RATE_0_xfm;`',   
    use_legacy_sql=False,location="US",
    dag=dag)

BITSP_ROY_CALC_REVENUE_MONTH= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_CALC_REVENUE_MONTH', 
    sql='CALL `PRES_STATEMENT_REV_SHARE_xfm;`',   
    use_legacy_sql=False,location="US",
    dag=dag)

BITSP_ROY_CALC_SUB_MONTH= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_CALC_SUB_MONTH', 
    sql='CALL `CALC_BUCKET_SUB_0_xfm;`',   
    use_legacy_sql=False,location="US",
    dag=dag)

BITSP_ROY_METRICS= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_METRICS', 
    sql='CALL `SUB_METRIC_MONTH_0_xfm;`',   
    use_legacy_sql=False,location="US",
    dag=dag)

BITSP_ROY_PARTIN_BREAKG_MONTH= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_PARTIN_BREAKG_MONTH', 
    sql='CALL `PARTNER_BREAKAGE_MONTH_0_xfm;`',   
    use_legacy_sql=False,location="US",
    dag=dag)

BITSP_ROY_CALC_FINAL_MONTH= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_CALC_FINAL_MONTH', 
    sql='CALL `PRES_STATEMENT_CALC_FINAL_xfm;`',   
    use_legacy_sql=False,location="US",
    dag=dag)

BITSP_NC_ROYALTY_MONTH= BigQueryExecuteQueryOperator(
    task_id='BITSP_NC_ROYALTY_MONTH', 
    sql='CALL `NC_ROYALTY_MONTH_0_xfm;`',   
    use_legacy_sql=False,location="US",
    dag=dag)

BITSP_ROY_LR_GL_FILE= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_LR_GL_FILE', 
    sql='CALL `GL_LABEL_ROYALTIES_MONTH_0_xfm;`',   
    use_legacy_sql=False,location="US",
    dag=dag)

BITSP_ROY_US_PRO_CALC= BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_US_PRO_CALC', 
    sql='CALL `PRO_USAGE_MONTH_F_xfm;`',   
    use_legacy_sql=False,location="US",
    dag=dag)
                                                         
end_task = BashOperator(
    task_id='end_task',
    bash_command='echo "job runs successfully" ',
    dag=dag)



BITSP_ROY_BUCKET_PROP_SHARE >> [BITSP_ROY_CALC_PER_PLAY_MONTH , BITSP_ROY_COUNT_SUB_MONTH] >> BITSP_ROY_CALC_FINAL_MONTH >> [BITSP_ROY_CALC_GROSS_REV_MONTH , BISTP_ROY_UPCHARGE_SUB_MONTH] >> [BITSP_ROY_CALC_DEDUCTION_MONTH , BITSP_ROY_ESCALATION_BLENDER] >> BITSP_ROY_UPCHARGE_PSM_RATE >> [BITSP_ROY_CALC_REVENUE_MONTH,BITSP_ROY_CALC_FINAL_MONTH] >> [BITSP_ROY_PSM_RATE_FINDER >> BITSP_ROY_REV_RATE_FINDER] >> [BITSP_ROY_UPCHARGE_PSM_RATE , BITSP_ROY_CALC_SUB_MONTH , BITSP_ROY_METRICS ]>> BITSP_ROY_CALC_REVENUE_MONTH >> [BITSP_ROY_CALC_SUB_MONTH , BITSP_ROY_METRICS] >> [BITSP_ROY_CALC_FINAL_MONTH , BIXFM_ROY_PUB_CLOSE_MONTH] >> BITSP_ROY_PARTIN_BREAKG_MONTH >>  BITSP_ROY_CALC_FINAL_MONTH >> BITSP_NC_ROYALTY_MONTH >> BITSP_ROY_LR_GL_FILE >> BITSP_ROY_US_PRO_CALC >>  end_task
