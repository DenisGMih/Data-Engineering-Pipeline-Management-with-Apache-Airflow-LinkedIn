
# IMPORTANT: Delete the old SQLite DB so it does not exist when we first start off

# IMPORTANT: Make sure that the airflow is the default installation (uses SQLite for metadata and not Postgres)


# https://airflow.apache.org/docs/apache-airflow/2.0.1/installation.html
# It is recomended to use python version 3.6, 3.7 or 3.8

# Airflow is tested with 3.7, 3.8, 3.9, 3.10


$ conda env list
# This will list all the enviroments present

# Now lets switch the enviroment from 3.9 to 3.7
$ conda activate /opt/anaconda3/envs/python37

$ python --version
# We are running Python 3.7.15

-------------------------------------------------------------------------------

# Note: This is the same set up as in the previous course BUT I have changed the metadata database that we are using. This will allow us to start with no users

# Show that we are using Postgres as the metadata database

# Show in the desktop that you have Postgrest running

# Click on the elephant and select "Open Postgres" and show you have it running

# Open up airflow.cfg and show the sql_alchemy_connection

sql_alchemy_conn = postgresql+psycopg2://postgres:Aspargilius123@localhost:5432/airflow_metadata_db


# Show that we are using the local executor

executor = LocalExecutor

# Close the file

-------------------------------------------------------------------------------

# In the terminal window

$ airflow version

$ cd /Users/loonycorn/airflow

$ airflow db init



$ airflow users list

# Should be no users


$ airflow scheduler

# If the below question is asked type "y" and hit the return key
# Please confirm database initialize (or wait 4 seconds to skip it). Are you sure? [y/N]

# Scheduler (as we saw above in cfg file we are using SequentialExecutor) monitors all tasks 
# and DAGs and stays in sync with a folder for all DAG objects to
# collects DAG parsing results to trigger active tasks

# Open another tab in the terminal
$ conda activate /opt/anaconda3/envs/python37

$ airflow webserver

# If the below question is asked type "y" and hit the return key
# Please confirm database initialize (or wait 4 seconds to skip it). Are you sure? [y/N]

--------------------------

# Go to localhost:8080

# Show the login page (we cannot log in since we do not have a user)





















