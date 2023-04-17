from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime
from pull_youtube_data import pull_data
from run_query_in_athena import run_query


# pip install apache-airflow
# pip install pandas
# pip install boto3
# pip install --upgrade google-api-python-client
# pip install --upgrade google-auth-oauthlib google-auth-httplib2


# create dag
with DAG(
    dag_id='pulling_youtube_data',
    schedule='@daily',
    start_date=datetime(year=2023, month=4, day=7), 
    catchup=False,
    tags=["youtube"]
) as dag:

    # pull Brazil video data
    pull_data_BR = PythonOperator(task_id='pull_video_data_BR',
                                python_callable=pull_data,
                                op_kwargs={'region_code': 'BR'})
    
    # pull India video data
    pull_data_IN = PythonOperator(task_id='pull_video_data_IN',
                                python_callable=pull_data,
                                op_kwargs={'region_code': 'IN'})
    
    # pull Indonesia video data
    pull_data_ID = PythonOperator(task_id='pull_video_data_ID',
                                python_callable=pull_data,
                                op_kwargs={'region_code': 'ID'})
    
    # pull Mexico video data
    pull_data_MX = PythonOperator(task_id='pull_video_data_MX',
                                python_callable=pull_data,
                                op_kwargs={'region_code': 'MX'})
    
    # pull United States video data
    pull_data_US = PythonOperator(task_id='pull_video_data_US',
                                python_callable=pull_data,
                                op_kwargs={'region_code': 'US'})
    
    # run query on all collected data
    run_query = PythonOperator(task_id='run_sql_query',
                                python_callable=run_query)
    
    # pull all data before running the query
    [pull_data_BR, pull_data_ID, pull_data_IN, pull_data_MX, pull_data_US] >> run_query


