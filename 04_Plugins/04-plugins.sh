-------------------------------------------------
# https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/plugins.html

# Open up "airflow.cfg" in Sublimetext

# Observe this particular line 

plugins_folder = /Users/loonycorn/airflow/plugins

# So we can create a plugin inside this particular path if we need to create in some
# other path we can edit in airflow.cfg and restart the scheduler and webserver

# Create a new folder in the airflow root called "plugins"

# Go to the Airflow UI navigation pane

Admin -> Plugin

# There are no plugins loaded

# Show empty_plugin.py in the airflow/plugins folder


from airflow.plugins_manager import AirflowPlugin

class EmptyPlugin(AirflowPlugin):
    name = "Empty Plugin"

    def on_load(*args, **kwargs):
        pass


# Stop and restart the scheduler and webserver

# Go to the Airflow UI navigation pane

Admin -> Plugin

# Observe now there is a plugin called "Empty Plugin"


---------------------------------------------------------------

# Notes:
# There are many types of plugin components that can be added to airflow as shown below

# appbuilder_menu_items allow you to add additional sections and links to the Airflow menu.
# flask_blueprints and appbuilder_views offer the possibility to build a Flask project on top of Airflow.
# operator_extra_links and global_operator_extra_links are ways to add links to Airflow task instances.
# macros expand upon existing Jinja templates using custom functions.
# timetables offer the option to register custom timetables that define schedules which cannot be expressed in CRON.
# executors add the possibility to use a custom executor in your Airflow instance.
# listeners are an experimental feature to add notifications for events happening in Airflow.


---------------------------------------------------------------

# Create a new plugin to add menu items in the UI

# company_info_plugin.py

from airflow.plugins_manager import AirflowPlugin

appbuilder_subitemA = {
    "name": "Linked In",
    "href": "https://www.linkedin.com/",
    "category": "Company Information",
}

appbuilder_subitemB = {
    "name": "Loonycorn",
    "href": "https://www.loonycorn.com/",
    "category": "Company Information",
}

appbuilder_topitem = {
    "name": "Plugin Documentation",
    "href": "https://airflow.apache.org/docs/apache-airflow/2.5.0/plugins.html"
}

class CompanyInfoPlugin(AirflowPlugin):
    name = "Company Information Plugin"

    appbuilder_menu_items = [
        appbuilder_subitemA,
        appbuilder_subitemB,
        appbuilder_topitem
    ]

    def on_load(*args, **kwargs):
        pass


# Stop and restart the scheduler and webserver

# Go to the Airflow UI

# Note the "Company Information" in the drop down menu

# Hover over it and click on "Linked In" -> this will go to the Linked In page

# Come back to the Airflow page

# Click on "Plugin Documentation" -> Show the plugin details

# Come back to the Airflow page

# Go to Admin -> Plugins

# Show the details of the second plugin here


------------------------------------------------------------------------------

# First let's show the CSV reader plugin running

# Set up all the code behind the scenes for this plugin

# Run the scheduler and webserver

# Go to the airflow UI

# Click on "Data" in the navigation bar

# Select the CSV reader

# Enter one file in the textbox

/Users/loonycorn/airflow/datasets/purchases.csv

# Click on "View CSV" and view the details

# Click on the back button

# Enter another file

/Users/loonycorn/airflow/datasets/customers.csv

# Click on "View CSV" and view the details

# There should be many empty values in this dataset

------------------------------------------------------------------------------

# Open up a Terminal window

# Show that under the datasets/ folder there are two files from the previous demo

customers.csv
purchases.csv 


# Now show the following files in this order


# In the plugins/ folder

csv_reader_plugin.py


# Show the contents of the file (I am not pasting it in here because it is too long)

# Under the plugins/templates/ folder show the two HTML files

CSVDisplay.html
getCSVPathURL.html

# Now run the scheduler and webserver again

# In the Airflow UI got to Data -> CSV Reader

# Enter

/Users/loonycorn/airflow/datasets/customers.csv

# Show the details


# Go to Admin -> Plugins and show the new plugin listed here


















