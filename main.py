from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import DAG

args = {
    'dag_id': 'stain_test',
    'run_date': '{{ ds }}',
    'schema_name': 'mydataset',
    'table_name': 'temp_delete',
    'source_table': 'fh-bigquery.reddit.subreddits'
}

INSERT_ROWS_QUERY = f"""
insert into {args['schema_name']}.{args['table_name']} 
select date(c.created_utc) as dt,
      c.num_comments,
      c.c_posts as posts,
      c.ups,
      c.downs
      ,STRUCT(cast(c.subr as string) as subr, c.num_comments as num_comments, c.c_posts as posts, c.ups as ups, c.downs as downs) as subreddit_metrics
 from {args['source_table']} c
 where date(c.created_utc)  = {args['run_date']}
"""

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2019, 11, 1),
    'schedule_interval': '0 09 1 * *',
    'max_active_runs': 1,
    'catchup': True,
    'concurrency': 1,
    'retries': 3,
    'retry_delay': timedelta(minutes=10)
}

with DAG(dag_id=args['dag_id'], default_args=default_args) as dag:

    start = DummyOperator(
        task_id='start_Dag'
    )

    insert_query_job = BigQueryInsertJobOperator(
        task_id="insert_query_job",
        configuration={
            "query": {
                "query": INSERT_ROWS_QUERY,
                "useLegacySql": False,
            }
        }
    )
    end = DummyOperator(
        task_id='End_dag'
    )

    start >> insert_query_job >> end



