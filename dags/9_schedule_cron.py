from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.trigger import CronTriggerTimetable


# You can schedule DAGS using cron

@dag(
    dag_id="schedule_cron_dag",
    start_date= datetime(year=2026, month=9, day=30, tz="Africa/Nairobi"),
    end_date= datetime(year=2026, month=10, day=1, tz="Africa/Nairobi"),
    schedule=CronTriggerTimetable("0 16 * * MON-FRI", timezone="Africa/Nairobi"),
    is_paused_upon_creation=False,
    catchup=True
)
def schedule_cron_dag():
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
schedule_cron_dag()