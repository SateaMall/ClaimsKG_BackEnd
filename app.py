from flask import Flask
from modules.dataframes import generate_global_dataframe
from modules.dataframes import generate_per_label_dataframe
from modules.statistics.summary import json_per_source_label
from modules.statistics.summary import dico_numbers_resume
from modules.statistics.summary import json_per_date1_label
from modules.statistics.summary import json_per_entity




app = Flask(__name__)

@app.route ("/update_label_df")
def update_label_df():
    return generate_per_label_dataframe()

@app.route ("/update_global_df")
def update_global_df():
    return generate_global_dataframe()

##TODO add rest api
@app.route ("/graph/perlabeldate")
def graph_per_label_date():
    return 5

@app.route ("/resume")
def resume():
    return dico_numbers_resume()

@app.route("/json_per_source_label")
def json():
    return json_per_source_label()

@app.route("/json_per_date1_label")
def json2():
    return json_per_date1_label()

@app.route("/json_per_entity")
def json3():
    return json_per_entity()



if __name__ == '__main__':
    app.run(debug=True)
