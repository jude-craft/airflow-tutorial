from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.events import EventsTimetable

special_dates = EventsTimetable(
    event_dates=[
    datetime(2026,9,1),
    datetime(2026,9,5),
    datetime(2026,9,14),
    datetime(2026,10,2)  
])

@dag(
    schedule=special_dates,
    start_date=datetime(2026,9,1, tz="Africa/Nairobi"),
    end_date=datetime(2026,10,2, tz="Africa/Nairobi"),
    catchup=True
)

def special_dates_dag():
    
    @task.python
    def special_event_task(**kwargs):
        execution_date = kwargs['logical_date']
        print(f"Running task for specila event on {execution_date}")
        
    special_event = special_event_task()
    
special_dates_dag()