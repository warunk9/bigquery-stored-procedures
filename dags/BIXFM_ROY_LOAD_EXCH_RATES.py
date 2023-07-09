import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator


dag = DAG(
    dag_id='BIXFM_ROY_LOAD_EXCH_RATES',
    description='A DAG that reads Python from GCS and runs',
    schedule_interval=None,
    start_date=airflow.utils.dates.days_ago(1),
)

BISH_ROY_REV_CP = BigQueryExecuteQueryOperator(
    task_id='BISH_ROY_REV_CP',
    sql='ssh_exec.sh "localhost","mkdir -p ${EDW_HOME}/landing/R2D2_WRK/monthly/{#2}; cp ${EDW_HOME}/partners/bi_finance/newroot/incoming/ExchangeRates{#1}.csv ${EDW_HOME}/landing/R2D2_WRK/monthly/{#2}/R2D2_WRK-NAPSTER_EXCHANGE_RATES-monthly-{#2}.dat;");',
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BITFL_R2D2_WRK_EXCHG_RATES = BigQueryExecuteQueryOperator(
    task_id='BITFL_R2D2_WRK_EXCHG_RATES',
    sql='fileloader.sh "R2D2_WRK", "R2D2_WRK","NAPSTER_EXCHANGE_RATES" ,"#today_yyyymmdd","monthly" ',
    use_legacy_sql=False,
    location="US",
    dag=dag
)

BITSP_ROY_EXCHANGE_RATES = BigQueryExecuteQueryOperator(
    task_id='BITSP_ROY_EXCHANGE_RATES',
    sql='CALL `stored_procs.ROY_CC_load_0_xfm`',
    use_legacy_sql=False,
    location="US",
    dag=dag
)


end_task = BashOperator(
    task_id='end_task',
    bash_command='echo "job runs successfully" ',
    dag=dag)

BISH_ROY_REV_CP >> BITFL_R2D2_WRK_EXCHG_RATES >> BITSP_ROY_EXCHANGE_RATES >>  end_task
