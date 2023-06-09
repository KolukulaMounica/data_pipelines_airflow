import pendulum
import logging
from airflow.decorators import dag, task

@dag(
    start_date=pendulum.now()   
)
def greet_flow_dag_legacy_123():

    @task()
    def hello_world_task():
        logging.info("Hello World!")

    hello_world=hello_world_task()

greet_dag=greet_flow_dag_legacy_123()