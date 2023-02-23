from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta


with DAG('dag', start_date=datetime(2021,1,1)):
    @task.kubernetes(
        image="python:3.8-slim-buster",
        name="k8s_test",
        namespace="default",
        in_cluster=True
    )
    def execute_in_k8s_pod():
        import time

        print("Hello from k8s pod")
        time.sleep(2)

    execute_in_k8s_pod()