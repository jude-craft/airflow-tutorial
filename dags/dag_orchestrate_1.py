from airflow.sdk import dag, task
import os

@dag(
    dag_id="first_orchestrator_dag",
)
def first_orchestrator_dag():
    @task.python
    def first_task():
        print("This is the first task")
        
    @task.python
    def second_task():
        print("This is the second task")
            
    @task.python
    def third_task():
        # Ensure the directory exists
        os.makedirs(os.path.dirname("/opt/airflow/logs/data"), exist_ok=True)
        
        # Simulate data fetching by writing to a file
        with open("/opt/airflow/logs/data/third_task.log", 'w') as f:
            f.write(f"Data processed successfully")
        
            
    # Defining task dependancies
    first = first_task()
    second = second_task()
    third = third_task()
    
    first >> second >> third
    
#Instantiating the DAG 
first_orchestrator_dag()