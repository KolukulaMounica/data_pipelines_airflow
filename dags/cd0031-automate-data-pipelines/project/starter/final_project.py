from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from final_project_operators.create_tables import CreateTablesOperator

from udacity.common import final_project_sql_statements
from udacity.common import final_project_sql_create



default_args = {
    'owner': 'udacity-mounica-k',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    create_redshift_tables = CreateTablesOperator(
    task_id="Initialize_tables",
    redshift_conn_id="redshift",
    sql=final_project_sql_create.table_initialization,
    skip=False
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table="staging_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket='udacity-mothikikm',
        s3_key='log-data',
        s3_format="FORMAT AS JSON 's3://udacity-mothikikm/log_json_path.json'"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table="staging_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket='udacity-mothikikm',
        s3_key='song-data',
        s3_format="JSON 'auto'"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplays',
        load_sql_stmt=final_project_sql_statements.SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        table='users',
        redshift_conn_id="redshift",
        truncate_table=True,
        load_sql_stmt=final_project_sql_statements.SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        table='songs',
        redshift_conn_id="redshift",
        truncate_table=True,
        load_sql_stmt=final_project_sql_statements.SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        table='artists',
        redshift_conn_id="redshift",
        truncate_table=True,
        load_sql_stmt=final_project_sql_statements.SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        table='time',
        redshift_conn_id="redshift",
        truncate_table=True,
        load_sql_stmt=final_project_sql_statements.SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        quality_checks=[
        { 'check_sql': 'SELECT COUNT(*) FROM public.songplays WHERE songplay_id IS NULL', 'expected_result': 0 }, 
        { 'check_sql': 'SELECT COUNT(*) FROM public.artists WHERE name IS NULL', 'expected_result': 0 },
        { 'check_sql': 'SELECT COUNT(*) FROM public.songs WHERE title IS NULL', 'expected_result': 0 },
        { 'check_sql': 'SELECT COUNT(*) FROM public.users WHERE first_name IS NULL', 'expected_result': 0 },
        { 'check_sql': 'SELECT COUNT(*) FROM public."time" WHERE day IS NULL', 'expected_result': 0 }
    ],
    )

    end_operator = DummyOperator(task_id='End_execution')

    start_operator >> create_redshift_tables
    create_redshift_tables >> stage_events_to_redshift >> load_songplays_table
    create_redshift_tables >> stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> load_user_dimension_table >> run_quality_checks
    load_songplays_table >> load_song_dimension_table >> run_quality_checks
    load_songplays_table >> load_artist_dimension_table >> run_quality_checks
    load_songplays_table >> load_time_dimension_table >> run_quality_checks
    run_quality_checks >> end_operator

final_project_dag = final_project()