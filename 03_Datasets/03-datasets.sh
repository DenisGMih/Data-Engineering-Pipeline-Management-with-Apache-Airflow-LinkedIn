----------------------------------------------------------------------
# Version 01
----------------------------------------------------------------------

# To say in voiceovers Pandas needs to be installed in the virtualenv running airflow

# Show the DAG for the producer of data

# Create a datasets/ folder and place the customers.csv in there

# IMPORTANT: Show the customers.csv file in Numbers

# Create an output/ folder that will hold the output

# producer_pipeline_customers.py

# Note that this reads in data with null values and produces a clean dataset

# Note the "outlets" specification in the last operator


import pandas as pd

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator


default_args = {
   'owner': 'loonycorn'
}

CUSTOMERS_DATA_PATH = '/Users/jananiravi/airflow/datasets/customers.csv'
CLEAN_CUSTOMERS_DATA_PATH = '/Users/jananiravi/airflow/output/clean_customers.csv'

clean_customers_dataset = Dataset(CLEAN_CUSTOMERS_DATA_PATH)

def reading_the_data():
    df = pd.read_csv(CUSTOMERS_DATA_PATH)

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

    df.to_csv(CLEAN_CUSTOMERS_DATA_PATH, mode='w', header=True, index=False)


with DAG(
    dag_id = 'producer_pipeline_customers',
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
        outlets = [clean_customers_dataset]
    )

    reading_the_data >> remove_null_values >> saving_cleaned_data



# Go to the Airflow UI

# Unpause the producer_pipeline_customers DAG

# Watch it run through successfully

# Show that under output/ we have the clean_customers.csv file

# Show the clean_customers.csv file in Numbers

# IMPORTANT: Delete the clean_customers.csv file in output/

---------------------------------------------------------------------------------

# Show the consumer DAG

# The schedule for this DAG waits for the dataset from the previous DAG to be available

# consumer_pipeline_customers.py

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

--------------------------

# In the Airflow UI

# Click on Datasets at the top

# Show there is 1 dataset listed

# Select that one dataset and show the flowchart

--------------------------

# In the Airflow UI have 2 tabs open

# In the first tab click through to the producer_pipeline_customers DAG - unpause the dag (if it is not already unpaused)

# In the second tab click through to the consumer_pipeline_customers DAG - unpause the DAG


# -- consumer_pipeline_customers
# Enable auto-refresh -- show that this DAG is just waiting on the dataset


# -- producer_pipeline_customers
# Trigger a run on the DAG and wait on this page till the second task becomes green

# Then switch over to consumer_pipeline_customers

# -- consumer_pipeline_customers
# Enable auto-refresh note that the DAG starts running as soon as the data is available


# Once this completes go to output/

# Show the contents of groupby_country.csv (note the counts by country e.g. China == 2)


------------------------------------------------------------------------------------------

# Now add some new data into customers.csv

991,Lindsey,lrigneyri@howstuffworks.com,125.71.193.235,Dongjiahe,China
992,Allis,amalyonrj@digg.com,30.68.64.54,Gulod,Philippines
993,Rebeca,rlendonrk@theguardian.com,213.79.25.157,Bourg-en-Bresse,France
994,Cristine,csircombrl@reddit.com,163.49.156.101,Sukpak,Russia
995,Delmer,dbrazelrm@ycombinator.com,197.23.187.208,Pagnag,China
996,Florry,fstocktonrn@businessweek.com,189.207.236.224,Nuasepu,Indonesia
997,Pru,pchannerro@goodreads.com,139.67.210.136,San Antonio,United States
998,Hurleigh,hsimmonrp@php.net,141.99.128.130,Piran,Slovenia
999,Quill,,239.64.107.128,Sobienie Jeziory,Poland
1000,Barbra,bgoadbierr@unesco.org,150.85.26.220,Khudāydād Khēl,Afghanistan


# Back to the Airflow UI (we still have the 2 tabs open)


# -- producer_pipeline_customers
# Trigger a run on the DAG and wait on this page till the second task becomes green
# Then switch over to consumer_pipeline_customers


# -- consumer_pipeline_customers
# Enable auto-refresh note that the DAG starts running again as soon as the new data is available


# Once this completes go to output/

# Show the contents of groupby_country.csv (note the counts by country e.g. China > 2)

# IMPORTANT: Delete all the files under output/

--------------------------------------------------------------------------------------------

# One more producer for purchases

# Under datasets/ show purchases.csv

# Show the file producer_pipeline_purchases.py


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


-----------------------------------------------------


# Now show a consumer that joins the customers and purchases data

# consumer_pipeline_join.py

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
CLEAN_PURCHASES_DATA_PATH = '/Users/jananiravi/airflow/output/clean_purchases.csv'
JOINING_DATA_PATH = '/Users/jananiravi/airflow/output/joined_data.csv'

clean_customers_dataset = Dataset(CLEAN_CUSTOMERS_DATA_PATH)
clean_purchases_dataset = Dataset(CLEAN_PURCHASES_DATA_PATH)


def reading_clean_customers_data():
    df = pd.read_csv(CLEAN_CUSTOMERS_DATA_PATH)

    print(df)

    return df.to_json()

def reading_clean_purchases_data():
    df = pd.read_csv(CLEAN_PURCHASES_DATA_PATH)

    print(df)

    return df.to_json()


def joining_the_data(ti):
    json_data = ti.xcom_pull(task_ids='reading_clean_customers_data')
    df_customers = pd.read_json(json_data)

    json_data = ti.xcom_pull(task_ids='reading_clean_purchases_data')
    df_purchases = pd.read_json(json_data)

    joined_data = pd.merge(df_customers, df_purchases, left_on='id', right_on='customer_id')

    joined_data.drop('id_x', axis=1, inplace=True)
    joined_data.drop('id_y', axis=1, inplace=True)
    joined_data.drop('customer_id', axis=1, inplace=True)

    joined_data.drop(['ip_address', 'city'], axis=1, inplace=True)

    joined_data['total_price'] = joined_data['price'] * joined_data['quantity']

    print(joined_data)

    return joined_data.to_json()


def saving_joined_data(ti):
    json_data = ti.xcom_pull(task_ids='joining_the_data')

    df = pd.read_json(json_data)

    df.to_csv(JOINING_DATA_PATH, mode='w', header=True, index=False)



with DAG(
    dag_id = 'consumer_pipeline_join',
    description = 'Executing a data-aware consumer pipeline with datasets',
    default_args = default_args,
    start_date = days_ago(1),
    schedule = [clean_customers_dataset, clean_purchases_dataset],
    tags = ['python', 'datasets', 'consumer', 'join']
) as dag:


    reading_clean_customers_data = PythonOperator(
        task_id = 'reading_clean_customers_data',
        python_callable = reading_clean_customers_data
    )

    reading_clean_purchases_data = PythonOperator(
        task_id = 'reading_clean_purchases_data',
        python_callable = reading_clean_purchases_data
    )
    
    joining_the_data = PythonOperator(
        task_id = 'joining_the_data',
        python_callable = joining_the_data
    )

    saving_joined_data = PythonOperator(
        task_id = 'saving_joined_data',
        python_callable = saving_joined_data
    )

    [reading_clean_customers_data, reading_clean_purchases_data] \
        >> joining_the_data >> saving_joined_data


-----------------------------------------------------------------------------

# Go to the Airflow UI

# Click on Datasets

# Show the flowchart

# Click on each dataset 

clean_purchases.csv 
clean_customers.csv

# Show the individual flowcharts


-----------------------------------------------------------------------------

# Open up 4 tabs in the airflow UI pointing to 4 DAGs

# Tabs should be in this order

producer_pipeline_customers
producer_pipeline_purchases # This one will be disabled
consumer_pipeline_customers
consumer_pipeline_join # This one will be disabled


# ---- producer_pipeline_customers
# Trigger this DAG and wait till everything turns green


# ----- consumer_pipeline_customers
# Show that this DAG is running - wait for it to complete (enable auto-refresh if needed)
# Go to the output/ folder and show the files exist there


# ----- consumer_pipeline_join
# Enable this join pipeline - it will not run (both datasets are not present)


# ----- producer_pipeline_purchases 
# Enable this so it runs and produces a result


# ----- consumer_pipeline_join
# Switch to this tab and show that the pipeline is running (enable auto-refresh)

# Once complete go to the output/ folder

# Open up the joined_data.csv file in Numbers and show the data


-----------------------------------------------------------------------

# Now go to the customers.csv file and add this data

100,Deanne,ddorracott2r@irs.gov,205.179.41.10,Bundoc,Philippines
101,Blanca,bmcamish2s@com.com,150.45.9.195,Rudniki,Poland
102,Cristie,cswatheridge2t@digg.com,48.254.66.190,Pali,Indonesia
103,Carin,cmauditt2u@lulu.com,68.55.180.49,Ijūin,Japan
104,Rand,rfarnish2v@youku.com,82.244.234.51,Beberon,Philippines
105,Fonsie,fdentith2w@alibaba.com,81.44.163.97,Novodvinsk,Russia


# Now go to the purchases.csv file and add this data


995,100,"Sauce - Bernaise, Mix",13.12,3
996,101,Parsley - Dried,87.0,17
997,102,Wine - Rosso Del Veronese Igt,16.17,7
998,103,Wine - Ruffino Chianti,67.05,15
999,9,Wine - Casillero Deldiablo,60.59,5
1000,104,"Juice - Pineapple, 48 Oz",31.03,1

-----------------------------------------------------------------------


# ---- producer_pipeline_customers
# Trigger this DAG and wait till everything turns green

# ----- consumer_pipeline_join
# Show that this one has not run (enable auto-refresh)

# ----- producer_pipeline_purchases 
# Trigger this so it runs and produces a result


# ----- consumer_pipeline_join
# Show that this is now running (enable auto-refresh)


# Once complete go to the output/ folder

# Open up the joined_data.csv file in Numbers and show the data

# Should be many more records than earlier






























































# purchase_history.py
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

PURCHASE_DATASET_PATH = '/Users/loonycorn/airflow/datasets/product_purchases.csv'
CLEAN_PURCHASE_DATASET_PATH = '/Users/loonycorn/airflow/output/clean_product_purchases.csv'



product_purchases_dataset = Dataset(PURCHASE_DATASET_PATH)
clean_product_purchases_dataset = Dataset(CLEAN_PURCHASE_DATASET_PATH)

def reading_the_dataset():
        df = pd.read_csv(PURCHASE_DATASET_PATH)
        print(df)
        return df.to_json()

def remove_null_values(ti):
    json_data = ti.xcom_pull(task_ids='reading_the_dataset')
    df = pd.read_json(json_data)
    df = df.dropna()

    print(df)

    return df.to_json()


def saving_to_local_filesystem(ti):
    version = Variable.get("version", default_var=None)

    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    df.to_csv(f'/Users/loonycorn/airflow/output/clean_product_purchases_{version}.csv', mode='w', header=True, index=False)

def upload_to_s3(ti):
    version = Variable.get("version", default_var=None)

    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    csv_bytes = df.to_csv(None, index=False)

    s3_hook = S3Hook(aws_conn_id='s3_connection')
    s3_hook.load_string(
        string_data=csv_bytes,
        key=f'clean_product_purchases_{version}.csv',
        bucket_name='diamond-bucket-loony',
        replace=True
    )

with DAG(
    dag_id = 'purchase_datasets',
    description = 'Running through Datasets in Airflow.',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['python', 'datasets', 'S3']
) as dag:
    checking_for_file = FileSensor(
        task_id = 'checking_for_file',
        filepath = PURCHASE_DATASET_PATH,
        poke_interval = 5,
        timeout = 60 * 10
    )

    reading_the_dataset = PythonOperator(
        task_id = 'reading_the_dataset',
        python_callable = reading_the_dataset
    )

    remove_null_values = PythonOperator(
        task_id = 'remove_null_values',
        python_callable = remove_null_values
    )

    saving_to_local_filesystem = PythonOperator(
        task_id = 'saving_to_local_filesystem',
        python_callable = saving_to_local_filesystem,
        outlets = [clean_product_purchases_dataset]
    )

    upload_to_s3 = PythonOperator(
        task_id = 'upload_to_s3',
        python_callable = upload_to_s3
    )

    checking_for_file >> reading_the_dataset >> remove_null_values >> [saving_to_local_filesystem, upload_to_s3]











# joining_the_data.py
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

CUSTOMER_INFO_DATASET_PATH = '/Users/loonycorn/airflow/datasets/customer_info.csv'
CLEAN_CUSTOMER_INFO_DATASET_PATH = '/Users/loonycorn/airflow/output/clean_customer_info.csv'

PURCHASE_DATASET_PATH = '/Users/loonycorn/airflow/datasets/product_purchases.csv'
CLEAN_PURCHASE_DATASET_PATH = '/Users/loonycorn/airflow/output/clean_product_purchases.csv'

JOINING_DATA_PATH = '/Users/loonycorn/airflow/output/joining_data.csv'


customer_dataset = Dataset(CUSTOMER_INFO_DATASET_PATH)
clean_customer_dataset = Dataset(CLEAN_CUSTOMER_INFO_DATASET_PATH)

product_purchases_dataset = Dataset(PURCHASE_DATASET_PATH)
clean_product_purchases_dataset = Dataset(CLEAN_PURCHASE_DATASET_PATH)

def start():
    print("START")

def reading_customer_dataset():
        version = Variable.get("version", default_var=None)

        df = pd.read_csv(f'/Users/loonycorn/airflow/output/clean_customer_info_{version}.csv')
        print(df)
        return df.to_json()

def reading_product_purchases_dataset():
        version = Variable.get("version", default_var=None)

        df = pd.read_csv(f'/Users/loonycorn/airflow/output/clean_product_purchases_{version}.csv')
        print(df)
        return df.to_json()


def joining_the_data(ti):
    json_data_cust = ti.xcom_pull(task_ids='reading_customer_dataset')
    df_cust = pd.read_json(json_data_cust)

    json_data_prod = ti.xcom_pull(task_ids='reading_product_purchases_dataset')
    df_prod = pd.read_json(json_data_prod)

    joining_data = pd.merge(df_cust, df_prod, left_on='id', right_on='customer_id')

    joining_data.drop('id_x', axis=1, inplace=True)
    joining_data.drop('id_y', axis=1, inplace=True)
    joining_data.drop('customer_id', axis=1, inplace=True)

    joining_data.drop(['ip_address', 'city'], axis=1, inplace=True)

    joining_data['total_price'] = joining_data['price'] * joining_data['quantity']

    print(joining_data)

    return joining_data.to_json()



def saving_data_locally(ti):
    version = Variable.get("version", default_var=None)
    
    json_data = ti.xcom_pull(task_ids='joining_the_data')
    df = pd.read_json(json_data)

    df.to_csv(f'/Users/loonycorn/airflow/output/joined_data{version}.csv', mode='w', header=True, index=False)



def update_version():
    version = Variable.get("version", default_var=None)

    new_version = int(version) + 1
    Variable.set("version", str(new_version))


with DAG(
    dag_id = 'joining_the_data',
    description = 'Running through Datasets in Airflow.',
    default_args = default_args,
    start_date = days_ago(1),
    schedule = [clean_customer_dataset, clean_product_purchases_dataset],
    tags = ['python', 'datasets', 'S3']
) as dag:

    start = PythonOperator(
        task_id = 'start',
        python_callable = start
    )

    reading_customer_dataset = PythonOperator(
        task_id = 'reading_customer_dataset',
        python_callable = reading_customer_dataset
    )

    reading_product_purchases_dataset = PythonOperator(
        task_id = 'reading_product_purchases_dataset',
        python_callable = reading_product_purchases_dataset
    )
    
    joining_the_data = PythonOperator(
        task_id = 'joining_the_data',
        python_callable = joining_the_data
    )

    saving_data_locally = PythonOperator(
        task_id = 'saving_data_locally',
        python_callable = saving_data_locally
    )

    update_version = PythonOperator(
        task_id = 'update_version',
        python_callable = update_version
    )

    start >> reading_customer_dataset >> joining_the_data >> saving_data_locally >> update_version
    start >> reading_product_purchases_dataset >> joining_the_data >> saving_data_locally >> update_version
























> In the ui toggle on the joining_the_data DAG and observe now it is waiting for 2 datasets



$ airflow dags test customer_datasets 2023-05-05







> Once complete do back to UI and refresh the page now observe we have the schedule
saying 1 out of 2 dataset 








$ airflow dags test purchase_datasets 2023-05-05





> Once complete do back to UI and refresh the page now observe we have the schedule
saying 2 out of 2 dataset 

Also observe now the joining_the_data is triggered and saving the csv file to output folder