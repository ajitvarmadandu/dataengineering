import io
import csv
import os
import pandas as pd
import airflow
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
#from airflow.utils.decorators import apply_defaults

class PostgresToS3Operator(BaseOperator):
#    @apply_defaults
    def __init__(self, postgres_conn_id, s3_conn_id, s3_bucket, s3_key, sql_query, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.sql_query = sql_query

    def execute(self, context):
        # Connect to Postgres and execute the query
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        self.log.info("Executing query: %s", self.sql_query)
        df = pg_hook.get_pandas_df(self.sql_query)

        if df.empty:
            self.log.info("No records found for the query.")
            return

        # Convert DataFrame to CSV in memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, quoting=csv.QUOTE_ALL)
        csv_buffer.seek(0)

        # Upload CSV to S3
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)
        self.log.info("Uploading data to S3 bucket: %s, key: %s", self.s3_bucket, self.s3_key)
        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key=self.s3_key,
            bucket_name=self.s3_bucket,
            replace=True
        )
        self.log.info("Upload complete.")

