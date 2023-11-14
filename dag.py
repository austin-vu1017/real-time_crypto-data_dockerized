from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer
import json, requests, time, logging

default_args = {
    'owner': 'avu_admin',
    'start_date': datetime(2023, 11, 1, 00, 00)
}

def get_api_data():
    response = requests.get('api.coincap.io/v2/assets')
    response.json()
    
    return response

def transform_data(r):
    data = {}
    
    return data

def stream_data():
    response = get_api_data()
    response = transform_data(response)
    
    producer = KafkaProducer(bootstrap_servers=['localhost:9021'], max_block_ms=5000)
    curr_time = time.time()
    
    while True:
        if time.time() > curr_time + 60:
            break
        try:
            response = get_api_data()
            response = transform_data(response)
            
            producer.send('crypto_added', json.dump(response).encode('utf-8'))
        except Exception as e:
            logging.error(f"ERROR: {e}")
    

with DAG('crypto_automation', default_args=default_args, schedule='@daily', catchup=False) as dag:
    streaming_task = PythonOperator(task_id='stream_data_from_api', python_callable=stream_data, op_args=dag)
    
stream_data()