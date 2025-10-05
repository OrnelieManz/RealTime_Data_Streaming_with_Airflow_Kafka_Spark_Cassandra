import pandas as pd
import six
import sys

from datetime import datetime
from airflow import DAG
#from airflow.providers.standard.operators.python import PythonOperator
#for kafka image, if the last version is not working try version 7.4.0
from airflow.operators.python import PythonOperator

if sys.version_info >= (3,12,0):
    sys.modules['kafka.vendor.six.moves'] = six.moves

default_args = {
    'owner': 'Ornelie',
    'start_date': datetime(2025,1,1,00,00)
}

def retrieve_data():
    import requests
    res = requests.get('https://financialdata.net/api/v1/commodity-prices?identifier=ZC&key=34e0dff7b74c53be95d89db466991e67')
    res = res.json()
    return res

def format_data(res, retrieval_type):
    if retrieval_type == 'latest':
        data = {}
        data['trading_symbol'] = res[0]['trading_symbol']
        data['date'] = res[0]['date']
        data['open'] = res[0]['open']
        data['high'] = res[0]['high']
        data['low'] = res[0]['low']
        data['close'] = res[0]['close']
        data['volume'] = res[0]['volume']
    elif retrieval_type == 'all':
        data = {}
        data['trading_symbol'] = [i['trading_symbol'] for i in res]
        data['date'] = [i['date'] for i in res]
        data['open'] = [i['open'] for i in res]
        data['high'] = [i['high'] for i in res]
        data['low'] = [i['low'] for i in res]
        data['close'] = [i['close'] for i in res]
        data['volume'] = [i['volume'] for i in res]
    
    return data  

#results = retrieve_data()
#df = pd.DataFrame(format_data(results,'latest'), index=[0])
#print(df.head())


def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    res = retrieve_data()
    res = format_data(res, retrieval_type='latest')
    #print(json.dumps(res, indent=3))
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
    producer.send('Daily_Zinc_data', json.dumps(res).encode('utf-8'))



with DAG('commodities_daily_data_streaming',
         default_args = default_args,
         schedule='@daily',
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id = 'stream_data_from_api',
        python_callable = stream_data
    )


#stream_data()
    