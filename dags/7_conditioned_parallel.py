from airflow.sdk import dag, task

@dag(
    dag_id="branch_dag",
)
def branch_dag():
    
    # Task 1
    
    @task.python
    def extract_task(**kwargs):
        print("Extracting data ....")
        ti = kwargs['ti']
        extracted_data_dict = {"api_extracted_data": [1,2,3],
                               "db_extracted_data":[4,5,6],
                               "s3_extracted_data":[7,8,9],
                               "weekend_flag": "false"
                               }
        ti.xcom_push(key='return_value', value=extracted_data_dict)
    
    # Task 2
    @task.python
    def transform_task_api(**kwargs):
        ti = kwargs['ti']
        api_extracted_data = ti.xcom_pull(task_ids='extract_task')['api_extracted_data']
        print(f"Transforming API data: {api_extracted_data}....")
        transformed_api_data = [i*10 for i in api_extracted_data]
        
        ti.xcom_push(key='return_value', value=transformed_api_data)
        
    # Task 3
    @task.python
    def transform_task_db(**kwargs):
        ti = kwargs['ti']
        db_extracted_data = ti.xcom_pull(task_ids='extract_task')['db_extracted_data']
        print(f"Transforming DB data: {db_extracted_data}....")
        transformed_db_data = [i*100 for i in db_extracted_data]
        
        ti.xcom_push(key='return_value', value=transformed_db_data)
        
    # Task 4
    @task.python
    def transform_task_s3(**kwargs):
        ti = kwargs['ti']
        s3_extracted_data = ti.xcom_pull(task_ids='extract_task')['s3_extracted_data']
        print(f"Transforming S3 data: {s3_extracted_data}....")
        transformed_s3_data = [i*1000 for i in s3_extracted_data]
        
        ti.xcom_push(key='return_value', value=transformed_s3_data)
        
    
    # Creating the Decider Node
    @task.branch
    def decider_task(**kwargs):
        ti = kwargs['ti']
        weekend_flag = ti.xcom_pull(task_ids='extract_task')['weekend_flag']
        if weekend_flag == "true":
            return 'no_load_task'
        else:
            return 'load_task'
        
            
    # Task 5
    @task.bash
    def load_task(**kwargs):
        print("Loading data to destination ...")
        api_data = kwargs['ti'].xcom_pull(task_ids='transform_task_api')
        db_data = kwargs['ti'].xcom_pull(task_ids='transform_task_db')
        s3_data = kwargs['ti'].xcom_pull(task_ids='transform_task_s3')
        
        return f"echo 'Loaded Data: {api_data}, {db_data}, {s3_data}'"
    
    # Task 6
    @task.bash
    def no_load_task(**kwargs):
        print("No loading on weekends....")
        return "echo 'No Load Task Executed'"
        
            
    # Defining task dependancies
    extract = extract_task()
    transform_api = transform_task_api()
    transform_db = transform_task_db()
    transform_s3 = transform_task_s3()
    load = load_task()
    no_load = no_load_task()
    
    extract >> [transform_api, transform_db, transform_s3] >> decider_task() >> [load, no_load]
    
#Instantiating the DAG 
branch_dag()