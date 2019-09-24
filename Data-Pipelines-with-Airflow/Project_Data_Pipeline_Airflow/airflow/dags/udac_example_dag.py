from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadTablesOperator, 
                               DataQualityOperator, RunSQLFileOperator)
from helpers import SqlQueries, CreateTables

#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Toly',
    'start_date': datetime(2018, 5, 1),
    'end_date': datetime(2018, 11, 30),
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'retries' : 3,
    'catchup': False,
    'depends_on_past': False
}

dag = DAG('udac_airflow_pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          max_active_runs = 3,
          catchup = False,
          schedule_interval ='@hourly'
          
        )

start_date = datetime.utcnow()

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


create_tables_task = RunSQLFileOperator(
    task_id = 'Create_tables',
    dag = dag,
    redshift_conn_id = 'redshift',
    sql_file = CreateTables.create_table_statement,
    database = 'dev' 
)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials = 'aws_credentials',
    table_name = 'staging_events',
    s3_bucket = 's3://udacity-dend/log_data',
    file_type = 'JSON',
    format_type = 's3://udacity-dend/log_json_path.json',
    execution_date = datetime(2018, 11, 1)


)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id = 'redshift',
    aws_credentials = 'aws_credentials',
    table_name = 'staging_songs',
    s3_bucket = 's3://udacity-dend/song_data/A/A/A',
    file_type = 'json',
    format_type = 'auto',
    

)

load_songplays_table = LoadTablesOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table_name = 'songplays',
    database = 'dev',
    sql_statement = SqlQueries.songplay_table_insert,
    table_type = 'fact'
)

load_user_dimension_table = LoadTablesOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table_name = 'users',
    database = 'dev',
    sql_statement = SqlQueries.user_table_insert,
    table_type = 'dim'

)

load_song_dimension_table = LoadTablesOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table_name = 'songs',
    database = 'dev',
    sql_statement = SqlQueries.song_table_insert,
    table_type = 'dim'
)

load_artist_dimension_table = LoadTablesOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table_name = 'artists',
    database = 'dev',
    sql_statement = SqlQueries.artist_table_insert,
    table_type = 'dim'
)

load_time_dimension_table = LoadTablesOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id = 'redshift',
    table_name = 'time',
    database = 'dev',
    sql_statement = SqlQueries.time_table_insert,
    table_type = 'dim'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    table_names = ['time','artists', 'songs', 'users', 'songplays'],
    database = 'dev'
    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Task ordering for the DAG tasks 
start_operator >> create_tables_task
create_tables_task >> stage_events_to_redshift 
create_tables_task >> stage_songs_to_redshift 
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table,
                         load_time_dimension_table] >> run_quality_checks >> end_operator

