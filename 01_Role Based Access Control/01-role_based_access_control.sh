# https://airflow.apache.org/docs/apache-airflow/stable/security/access-control.html

# Open a terminal window in fullscreen in the Conda environment

$ airflow users list

# There should be no users


$ airflow users create -h
# Observe here we have got a template as to how to create a user

# Here observe the different roles we can provide for the users

# ROLE of the user. Existing roles include Admin, User, Op, Viewer, and Public

# Public – Role with permissions for unauthenticated users. Don’t have any permissions.
# Viewer – Can only use the Airflow Web UI to see the status but cannot launch DAGs or change anything else. This is also Read-only access.
# Users – Able to perform DAG related tasks – marking DAGs/tasks as failed or successful. Able to launch DAGs etc
# Ops – Ability to modify the configuration
# Admin – Has all the permissions. Has access to do everything.


$ airflow users create \
-e cloud.user@loonycorn.com \
-f CloudUser \
-l Loonycorn \
-p password \
-r Admin \
-u cloud.user 


$ airflow users list

# id | username    | email                     | first_name | last_name | roles 
# ===+=============+===========================+============+===========+=======
# 1  | cloud.user  | cloud.user@loonycorn.com  | CloudUser  | Loonycorn | Admin 


----------------------------------------------

# Go over to the Airflow UI 

# Login 

Username: cloud.user
Password: password


# Can see the main UI

# Now Just log out from the top-right

# Login once again

# Now from the navigation bar 
Goto "Security" -> "List Users"

# We can see there is one user "cloud.user"

# Now if we click "Show Record" (magnifing glass) we can see indepth about the user
# Expand all the sections and show (note that there should be 2 logins)


# Go back to the "List Users" page

# Click on the Edit icon and show that you can change the details of the user


# From the navigation bar 
Goto "Security" -> "List Roles"


# Show all the possible types of roles and the permissions associated with each type of role

# Click Cmd + F to search for the term "on Users"

# Note that only the Admin role has ability to create and work with users

# Click on the Edit icon for the Admin role and show all the permissions


# From the navigation bar 
Goto "Security" -> "User Statistics"

# Show the graph of Logins (should be 2)


------------------------------------------------

# Now let's go back to the terminal window and create some more users with different roles

$ airflow users create \
-e public.user@loonycorn.com \
-f Public \
-l User \
-p password \
-r Public \
-u public.user

# Note that no permissions are added for this role



$ airflow users create \
-e viewer.user@loonycorn.com \
-f Viewer \
-l User \
-p password \
-r Viewer \
-u viewer.user



$ airflow users list

# id | username    | email                     | first_name | last_name | roles 
# ===+=============+===========================+============+===========+=======
# 1  | cloud.user  | cloud.user@loonycorn.com  | CloudUser  | Loonycorn | Admin 
# 2  | public.user | public.user@loonycorn.com | Public     | User      | Public
# 3  | viewer.user | viewer.user@loonycorn.com | Viewer     | User      | Viewer


# Go to the brower -> localhost:8080

username -> cloud.user
password -> password


# We have to use this Admin user because only the Admin can create users

Now click "+" button 

First Name -> User
Last Name -> User
Username -> user.user
Is Active -> [TICK]
Email -> user.user@loonycorn.com
Role -> Type in U and select User 
Password -> password
Confirm Password -> password

> Click "SAVE"

# Observe now a new user with username user.user is created

# Also observe if we want we can add multiple roles to the same user like 
# the user has the role of both say Viewer and Ops ... etc. But for this demo
# we will show just one



Now click "+" button 

First Name -> Op
Last Name -> User
Username -> op.user
Is Active -> [TICK]
Email -> op.user@loonycorn.com
Role -> Type O and select O
Password -> password
Confirm Password -> password

> Click "SAVE"


------------------------------------------------------------------

# Now let's have some DAGs that we can play around with

# The first DAG does simple branching and uses a Variables to make a choice

# Show simple_branching_with_variable.py in VSCode


from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable


default_args = {
   'owner' : 'loonycorn'
}

def choose_branch():
    choice = Variable.get("choice", default_var=False)

    return choice

def branch(ti):
    choice = ti.xcom_pull(task_ids='taskChoose')

    if choice == 'C':
        return 'taskC'
    elif choice == 'E':
        return 'taskE'

def task_c():
    print("TASK C executed!")


with DAG(
    dag_id = 'simple_branching_with_variable',
    description = 'Simple branching pipeline using variables',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['pipeline', 'branching', 'variables']
) as dag:

    taskA = BashOperator(
        task_id = 'taskA',
        bash_command = 'echo TASK A has executed!'
    )

    taskChoose = PythonOperator(
        task_id = 'taskChoose',
        python_callable = choose_branch
    )

    taskBranch = BranchPythonOperator(
        task_id = 'taskBranch',
        python_callable = branch
    )

    taskC = PythonOperator(
        task_id = 'taskC',
        python_callable = task_c
    )

    taskD = BashOperator(
        task_id = 'taskD',
        bash_command = 'echo TASK D has executed!'
    )

    taskE = EmptyOperator(
        task_id = 'taskE',
    )


taskA >> taskChoose >> taskBranch >> [taskC, taskE]

taskC >> taskD


# Open up Admin -> Variables in a new tab

# > Admin -> Variables -> Add

Key: choice
Value: C
Save


# Now go back to the simple_branching_with_variable DAG

# Go to the Graph view (zoom out if needed)

# Enable the DAG and it will automatically run through once

# Wait till it runs through


--------------------------------------------------------------------------

# Now show the second DAG which performs some basic SQLlite operations (this will use a Connection to the database)


from airflow import DAG

from airflow.operators.sqlite_operator import SqliteOperator

from datetime import date, datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner' : 'loonycorn'
}

with DAG(
    dag_id = 'simple_sql_pipeline',
    description = 'Pipeline using SQL operators',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['pipeline', 'sql']
) as dag:
    create_table = SqliteOperator(
        task_id = 'create_table',
        sql = r"""
            CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(50) NOT NULL,
                    age INTEGER NOT NULL,
                    city VARCHAR(50),
                    is_active BOOLEAN DEFAULT true,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
    )


    insert_values_1 = SqliteOperator(
        task_id = 'insert_values_1',
        sql = r"""
            INSERT INTO users (name, age, is_active) VALUES 
                ('Julie', 30, false),
                ('Peter', 55, true),
                ('Emily', 37, false),
                ('Katrina', 54, false),
                ('Joseph', 27, true);
        """,
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
    )

    insert_values_2 = SqliteOperator(
        task_id = 'insert_values_2',
        sql = r"""
            INSERT INTO users (name, age) VALUES 
                ('Harry', 49),
                ('Nancy', 52),
                ('Elvis', 26),
                ('Mia', 20);
        """,
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
    )

    display_result = SqliteOperator(
        task_id = 'display_result',
        sql = r"""SELECT * FROM users""",
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
        do_xcom_push = True
    )


    drop_table = SqliteOperator(
        task_id = 'drop_table',
        sql = r"""
            DROP TABLE users;
        """,
        sqlite_conn_id = 'my_sqlite_conn',
        dag = dag,
    )

create_table >> [insert_values_1, insert_values_2] >> display_result >> drop_table



# Create a folder called "database" under airflow/

# > Goto "localhost:8080"
# > Click on the "Admin" tab in the top navigation bar.
# > Click on "Connections" in the dropdown menu.
# > Click on the "Create" button on the right side of the page.

Conn Id: my_sqlite_conn
Conn Type: sqlite
Host: /Users/loonycorn/airflow/database/my_sqlite.db

# > Click "Test" 
# > Click "Save"

# Note: "my_sqlite.db" will be created under database/

# Can go to VSCode and show this my_sqlite.db


# Now back to the AIrflow UI

# Go to the Graph view (zoom out if needed)

# Enable the DAG and it will automatically be triggered

# Let it run through

# click on display_result and show the XCom

--------------------------------------------------------------------------

> logout and login as a "public.user"
# Your user has no roles and/or permissions!
# Unfortunately your user has no roles, and therefore you cannot use Airflow.

# Please contact your Airflow administrator (authentication may be misconfigured) or log out to try again.

 # We will fix this after


--------------------------------------------------------------------------

> Click "logout" and login as "viewer.user"
# Observe in the navigation bar now there are only limited options

# Hover over Browse and Docs and show some options are available

# Click on Browse -> DAG Runs

# Click on Browse -> Jobs

# Go back to the DAGs page

# Hover over the enabled/disabled toggle and show that we cannot change this

# Click on simple_branching_with_variable and show that we can see the Grid

# Go to the Graph view (zoom out if needed)

# Click on Code and we can see the code

# Hover over the arrow on the top right and show we CANNOT trigger this DAG

# Click on the branching task and show we can see the Logs

# Click on XCom and show that we can see this as well

# This role has read-only access to the Airflow web UI and can see the 
# status of DAGs but cannot launch or modify them


--------------------------------------------------------------------------

> Click "logout" and login as "user.user"


# Show the top navigation menu

# Click on Browse -> DAG Runs (options are same as the viewer)

# Click on Browse -> Jobs (options are same as the viewer)

# Note that this user cannot create any connections, variables..etc

# Try to pause and unpause a DAG it should work

# Click through to simple_sql_pipeline

# Go to the Graph view (zoom out if needed)

# Enable Auto-refresh and trigger a DAG run (by clicking on the play button top right)

# Wait for the run to complete

# Now go to Grid view and select the last Run of this DAG

# Note that the "Mark Failure" and "Mark Success" options show up

# Mark this run as successful


--------------------------------------------------------------------------

> Click "logout" and login as "op.user"


# Show the top navigation menu

# Go to Admin -> Variables

Edit the "choice" variable -> set the value to "E"


# Go to the DAGs view and select the simple_branching_with_variables

# Go to the Graph view (zoom out if needed)

# Trigger a DAG run

# Show that E is executed

# But observe they dont have the ability to create or delete any user


# Admin can do everything


--------------------------------------------------------------------------


* Admin > Op > Users > Viewer > Public

> Click "logout" and login as "cloud.user"


# Now from the navigation bar 
Click "Security" -> "Actions"
# Here we can see all the actions that can be performed

Click "Security" -> "Resources"
# Here we can observe all the resources avaliable. If we go to the last page
# we can observe our previously created DAGs as well

Click "Security" -> "Permissions"
# Here we can observe all the "Actions" mapped to the "Resources"
# Like for example we can edit password, we can delete users ... etc
# If we goto the last page we can also see like we can create, delete, edit ... for
# the DAGs that we have created like catchup_false, DAG_2 also. 

# This gives us all the possible permissions on the different resources (combination of actions + resources)


Now goto "Security" -> "List Roles"
# Here observe for each roles created there are a bunch of Permissions associated
# with it this is the same Permissions what we saw before from "Security" -> "Permissions"

# Click on the magnifying glass for Public

# Click on List Users and we can see the corresponding users

--------------------------------------------------------------------------
# Adding permissions for the public.user


Goto "Security" -> "List Roles"

# If we observe here there is no permissions set for public users that is why we where getting that error


# > Click on edit "Public" user 

Permissions -> can read on DAG Runs, can read on Jobs, can read on Task Instances,
				can read on DAGs, can read on DAG Dependencies,
				can read on Task Logs, can read on Website,

# Click "SAVE"


> Now logout as "cloud.user" 

# Show that we can see the DAGs without logging in

# Click on one of the DAGs and show that we can see it's details in the Grid view


> login as "public.user"

# Observe now we can access the website

# Click on any DAG

# Show the Grid view

# Show the Graph view

# Click on Code -> we cannot see the code

# Click on Audit Logs -> we cannot see that

--------------------------------------------------------------

> Now logout as "public_user" and login as "cloud.user"

> Go to "Security" -> "List Roles"

# Now lets create our own role 

> Click on the "+" button

Name -> Loony
Permissions -> can read on DAG Runs, can read on Jobs, can read on Task Instances,
				can read on DAGs, can read on DAG Dependencies,
				can read on Task Logs, can read on Website, can read on dag code, 
				can read on audit log, can delete on DAG:simple_sql_pipeline,
				can read on DAG:simple_sql_pipeline, can edit on DAG:simple_sql_pipeline

Click "SAVE"

# Observe the role "Loony" is now added

# Observe here we are specifically mentioning that we have the permissions to delete, read and edit simple_sql_pipeline

# Now from the navigation bar 
Goto "Security" -> "List Users"

Now click "+" button 

First Name -> Loony
Last Name -> User
Username -> loony.user
Is Active -> [TICK]
Email -> loony.user@loonycorn.com
Role -> Type L and then select Loony
Password -> password
Confirm Password -> password

> Click "SAVE"

-----------------------------------------------------------------------

> Now logout as "cloud.user" and login as "loony.user"


# Show that we can pause and unpause the simple_sql_pipeline

# But we cannot do the same for the simple_branching_with_variable

# Click through and show that we can see the Grid and Graph view for simple_branching_with_variable




