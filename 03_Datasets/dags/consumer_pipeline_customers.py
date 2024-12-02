from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import pandas as pd

from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable


default_args = {
   'owner': 'loonycorn'
}

CLEAN_CUSTOMERS_DATA_PATH = '/Users/jananiravi/airflow/output/clean_customers.csv'
GROUPBY_COUNTRY_DATA_PATH = '/Users/jananiravi/airflow/output/groupby_country.csv'

clean_customers_dataset = Dataset(CLEAN_CUSTOMERS_DATA_PATH)


def reading_clean_data():
    df = pd.read_csv(CLEAN_CUSTOMERS_DATA_PATH)

    print(df)

    return df.to_json()


def groupby_country(ti):
    json_data = ti.xcom_pull(task_ids='reading_clean_data')

    df = pd.read_json(json_data)

    groupby_country_df = df.groupby('country').size(). \
        reset_index(name='count').sort_values(by='count', ascending=False)

    print(groupby_country_df)

    return groupby_country_df.to_json()


def saving_grouped_data(ti):
    json_data = ti.xcom_pull(task_ids='groupby_country')

    df = pd.read_json(json_data)

    df.to_csv(GROUPBY_COUNTRY_DATA_PATH, mode='w', header=True, index=False)



with DAG(
    dag_id = 'consumer_pipeline_customers',
    description = 'Executing a data-aware consumer pipeline with datasets',
    default_args = default_args,
    start_date = days_ago(1),
    schedule = [clean_customers_dataset],
    tags = ['python', 'datasets', 'consumer']
) as dag:


    reading_clean_data = PythonOperator(
        task_id = 'reading_clean_data',
        python_callable = reading_clean_data
    )
    
    groupby_country = PythonOperator(
        task_id = 'groupby_country',
        python_callable = groupby_country
    )

    saving_grouped_data = PythonOperator(
        task_id = 'saving_grouped_data',
        python_callable = saving_grouped_data
    )

    reading_clean_data >> groupby_country >> saving_grouped_data

