from airflow.decorators import dag, task
from datetime import datetime, timedelta


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

@task.kubernetes(image="python:3.8-slim-buster", namespace="default", in_cluster=True)
def print_pattern():
    n = 5
    for i in range(0, n):
        # inner loop to handle number of columns
        # values changing acc. to outer loop
        for j in range(0, i + 1):
            # printing stars
            print("* ", end="")

        # ending line after each row
        print("\r")

execute_in_k8s_pod_instance = execute_in_k8s_pod()
print_pattern_instance = print_pattern()
execute_in_k8s_pod_instance >> print_pattern_instance

