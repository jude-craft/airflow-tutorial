from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.interval import CronDataIntervalTimetable

@dag(
    schedule=CronDataIntervalTimetable("@daily", timezone="Africa/Nairobi"),
    start_date=datetime(year=2026, month=10, day=1, tz="Africa/Nairobi"),
    end_date=datetime(year=2020, month=10, day=5, tz="Africa/Nairobi"),
    catchup=True
)

def incremental_load_dag():
    
    @task.python
    def incremental_load_fetch(**kwargs):
        date_interval_start = kwargs['data_interval_start']
        date_interval_end = kwargs['data_interval_end']
        print(f"Fetching data from {date_interval_start} to {date_interval_end}")
        
    @task.python
    def incremental_load_process():
        return "echo 'Processing incremental data from {{data_interval_start}} to {{data_interval_end}}'"
    
    
    fetch_task = incremental_load_fetch()
    process_task = incremental_load_process()
    
    fetch_task >> process_task

# Instantiating the DAG
incremental_load_dag()