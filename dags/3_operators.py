from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator

@dag(
    dag_id="operators_dag",
)
def operators_dag():
    @task.python
    def first_task():
        print("This is the first task")
        
    @task.python
    def second_task():
        print("This is the second task")
            
    
    @task.bash
    def bash_task_modern() -> str:
        return "echo https://airflow.apache.org/"
    
    bash_task_old_school = BashOperator(
    task_id="bash_task_old_school",
    bash_command="echo https://airflow.apache.org/",
)
            
    # Defining task dependancies
    first = first_task()
    second = second_task()
    bash_modern = bash_task_modern()
    bash_old_school = bash_task_old_school
    
    first >> second >> bash_modern >> bash_old_school
    
#Instantiating the DAG 
operators_dag()