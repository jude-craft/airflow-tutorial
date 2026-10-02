from airflow.sdk import dag, task
from pendulum import datetime


# You can schedule DAGS using presets like @daily, @hourly, @weekly, @monthly, @yearly, @once, @never

@dag(
    dag_id="first_schedule_dag",
    start_date= datetime(year=2026, month=9, day=1, tz="Africa/Nairobi"),
    schedule="@daily",
    is_paused_upon_creation=False
)
def first_schedule_dag():
    @task.python
    def first_task():
        print("This is the first task")
        
    @task.python
    def second_task():
        print("This is the second task")
            
    @task.python
    def third_task():
        print("This is the third task")
        
            
    # Defining task dependancies
    first = first_task()
    second = second_task()
    third = third_task()
    
    first >> second >> third
    
#Instantiating the DAG 
first_schedule_dag()