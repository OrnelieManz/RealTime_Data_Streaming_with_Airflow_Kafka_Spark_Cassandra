from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def print_hi(name):
    print(f'hi, {name}')

if __name__ == '__main__':
    print_hi('Got_it')
