from airflow.plugins_manager import AirflowPlugin

class EmptyPlugin(AirflowPlugin):
    name = "Empty Plugin"

    def on_load(*args, **kwargs):
        pass
