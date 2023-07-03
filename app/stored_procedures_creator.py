from google.cloud import bigquery
import os


def create_stored_procedure(sql_file_path):
    """Creates a stored procedure in BigQuery.

    Args:
      sql_file_path: The path to the SQL file that contains the stored procedure definition.
    """

    if not os.path.exists(sql_file_path):
        print(f"[ERROR]: File '{sql_file_path}' does not exist")
        return

    with open(sql_file_path, "r") as f:
        sql_statement = f.read()

    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    job_config.query_parameters = [
        bigquery.ScalarQueryParameter("sql_statement", "STRING", sql_statement)
    ]
    try:
        job = client.query(
            query=" {}".format(sql_statement),
            job_config=job_config,
        )

        job.result()
    except Exception as e:
        print(e)
        print("[ERROR]: Failed to create stored procedure")


if __name__ == "__main__":
    sql_file = "sqls/calc_bucket_sub_0_xfm.sql"
    create_stored_procedure(sql_file)
