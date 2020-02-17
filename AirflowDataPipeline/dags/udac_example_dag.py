from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, CreateTablesOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'oleh',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          #start_date=default_args['start_date']
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# this ensures that `songplays` table is created after all other tables were created
initialize_tables = [
    SqlQueries.create_staging_events,
    SqlQueries.create_staging_songs,
    SqlQueries.create_table_users,
    SqlQueries.create_table_time,
    SqlQueries.create_table_artists,
    SqlQueries.create_table_songs,
    SqlQueries.create_table_songplays,
]

create_tables = CreateTablesOperator(
    task_id='create_tables',
    dag=dag,
    redshift_conn_id="redshift",
    queries_to_run=initialize_tables,  # list of tables to create
    table_names=["staging_events", "staging_songs", "users", "time", "artists", "songs", "songplays"]
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="public.staging_events",
    #create_sql_stmt=SqlQueries.create_staging_events,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    data_format="s3://udacity-dend/log_json_path.json",
    start_date=default_args['start_date'],
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="public.staging_songs",
    #create_sql_stmt=SqlQueries.create_staging_songs,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A",
    region="us-west-2",
    data_format="auto",
    start_date=default_args['start_date'],
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table = "public.songplays",
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table = "public.users",
    sql=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table = "public.songs",
    sql=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table = "public.artists",
    sql=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table = "public.time",
    sql=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    provide_context=True,
    tables=["public.songplays", "public.users", "public.songs", "public.artists", "public.time"],
    tests=["SELECT COUNT(*) FROM {}"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables
create_tables >> stage_events_to_redshift
create_tables >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
