from airflow.sdk import dag, task

from pendulum import datetime, duration
from airflow.timetables.trigger import DeltaTriggerTimetable


# You can schedule DAGS using delta

@dag(
    dag_id="schedule_delta_dag",
    start_date= datetime(year=2026, month=9, day=30, tz="Africa/Nairobi"),
    end_date= datetime(year=2026, month=10, day=1, tz="Africa/Nairobi"),
    schedule=DeltaTriggerTimetable(duration(days=3)),
    is_paused_upon_creation=False,
    catchup=True
)
def schedule_delta_dag():
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
schedule_delta_dag()