from airflow.plugins_manager import AirflowPlugin

from flask import Blueprint, request
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from flask_wtf import FlaskForm

from wtforms import StringField, SubmitField

import csv

my_blueprint = Blueprint(
    "csv_reader_plugin",
    __name__,
    template_folder="templates"
)

def get_data_type(column):
    for value in column:
        try:
            int(value)
        except ValueError:
            pass
        else:
            return int
        try:
            float(value)
        except ValueError:
            return str
        else:
            return float


def get_unique_value_count(column):
    
    unique_items = set(column)
    
    unique_items.discard('')

    return len(unique_items)



def get_unique_values(column):
    unique_items = set(column)
    
    unique_items.discard('')

    return ", ".join(str(item) for item in unique_items)



class CSVForm(FlaskForm):
    path = StringField(
        "Enter the CSV file path:", 
        render_kw={'style': 'width: 80ch'})
    
    submit = SubmitField("View CSV")



class CSVReaderView(AppBuilderBaseView):

    default_view = "getCSVPathURL"

    @expose("/getcsvpath")
    def getCSVPathURL(self):
        
        form = CSVForm()

        return self.render_template("getCSVPathURL.html", form=form)
    
    @expose("/csvdisplay", methods=["GET", "POST"])
    def displayCSVFile(self):

        form = CSVForm()
        
        if request.method == "POST":
        
            if form.validate_on_submit():
                path = form.path.data
                csv_data = self.read_csv_data(path)

                column_overview = []
                
                for i, header in enumerate(csv_data[0]):
                    column = [row[i] for row in csv_data[1:]]

                    data_type = get_data_type(column)
                    unique_value_count = get_unique_value_count(column)
                    unique_values = get_unique_values(column)

                    column_overview.append(
                        (header, data_type, unique_value_count, unique_values))
                

                return self.render_template(
                    "CSVDisplay.html", 
                    csv_data=csv_data, 
                    column_overview=column_overview)
        
        return self.render_template("getCSVPathURL.html", form=form)

    def read_csv_data(self, path):
        csv_data = []

        try:
            with open(path, 'r') as file:
                csv_reader = csv.reader(file)
                for row in csv_reader:
                    csv_data.append(row)
        except IOError:
            pass
        return csv_data

csv_reader_view = CSVReaderView()

csv_reader_view_package = {
    "name": "CSV Reader",
    "category": "Data",
    "view": csv_reader_view,
}

class CSVReaderPlugin(AirflowPlugin):
    name = "CSV Reader Plugin"

    flask_blueprints=[my_blueprint]
    appbuilder_views = [csv_reader_view_package]

    def on_load(self, *args, **kwargs):
        pass













