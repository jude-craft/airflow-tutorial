import pendulum
from airflow.sdk import dag
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator 

# Define timezone
local_tz = pendulum.timezone("Africa/Nairobi")

@dag(
    dag_id="parent_orchestrator_dag",
    start_date=pendulum.datetime(2026, 10, 2, tz=local_tz),
    schedule=None,  # Set to None for manual triggering
)
def parent_orchestrator_dag():
    
    trigger_first_dag = TriggerDagRunOperator(
        task_id="trigger_first_orchestrator_dag",
        trigger_dag_id="first_orchestrator_dag",
        wait_for_completion=True,  # Optional: Wait for the first DAG to complete before proceeding
    )
    
    trigger_second_dag = TriggerDagRunOperator(
        task_id="trigger_second_orchestrator_dag",
        trigger_dag_id="second_orchestrator_dag",
        wait_for_completion=True,  # Optional: Wait for the second DAG to complete before proceeding
    )
    
    # Dependencies
    trigger_first_dag >> trigger_second_dag
    
# Instantiate the DAG
parent_orchestrator_dag()