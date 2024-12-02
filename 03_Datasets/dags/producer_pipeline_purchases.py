import pandas as pd

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator


default_args = {
   'owner': 'loonycorn'
}

PURCHASES_DATA_PATH = '/Users/jananiravi/airflow/datasets/purchases.csv'
CLEAN_PURCHASES_DATA_PATH = '/Users/jananiravi/airflow/output/clean_purchases.csv'

clean_purchases_dataset = Dataset(CLEAN_PURCHASES_DATA_PATH)

def reading_the_data():
    df = pd.read_csv(PURCHASES_DATA_PATH)

    print(df)

    return df.to_json()

def remove_null_values(ti):
    json_data = ti.xcom_pull(task_ids='reading_the_data')

    df = pd.read_json(json_data)

    df = df.dropna()

    print(df)
    
    return df.to_json()


def saving_cleaned_data(ti):

    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    df.to_csv(CLEAN_PURCHASES_DATA_PATH, mode='w', header=True, index=False)


with DAG(
    dag_id = 'producer_pipeline_purchases',
    description = 'Executing a data-aware producer pipeline with datasets',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['python', 'datasets', 'producer']
) as dag:

    reading_the_data = PythonOperator(
        task_id = 'reading_the_data',
        python_callable = reading_the_data
    )

    remove_null_values = PythonOperator(
        task_id = 'remove_null_values',
        python_callable = remove_null_values
    )

    saving_cleaned_data = PythonOperator(
        task_id = 'saving_cleaned_data',
        python_callable = saving_cleaned_data,
        outlets = [clean_purchases_dataset]
    )

    reading_the_data >> remove_null_values >> saving_cleaned_data






