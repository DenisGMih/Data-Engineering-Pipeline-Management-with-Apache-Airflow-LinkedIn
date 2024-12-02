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