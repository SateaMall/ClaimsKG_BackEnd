from flask import Flask
from modules.dataframes import generate_global_dataframe
from modules.dataframes import generate_per_label_dataframe
from modules.statistics.summary import list_resume_claims_per_source_label
from modules.statistics.summary import list_resume_borne_date1_date2
from modules.statistics.summary import list_resume_borne_entities
from modules.statistics.summary import list_resume_borne_source
from modules.statistics.summary import dico_new_data
from modules.statistics.summary import list_resume_claims_per_topics
from modules.statistics.summary import list_resume_borne_date1_date2_entity

from modules.statistics.summary import dico_numbers_resume
from flask_cors import CORS
from modules.statistics.summary import list_resume_claims_per_date_label
from modules.statistics.summary import entity

app = Flask(__name__)
CORS(app)

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
    return dico_new_data()

@app.route("/json_per_source_label")
def json():
    return list_resume_claims_per_source_label()

@app.route("/json_per_date1_label")
def json2():
    return list_resume_claims_per_date_label()

@app.route("/json_per_entity")
def json3():    
    return entity()

@app.route("/json_born_date")
def json4():
    return list_resume_borne_date1_date2()

@app.route("/json_born_entity")
def json5():
    return list_resume_borne_entities()

@app.route("/json_born_source")
def json6():
    return list_resume_borne_source()

@app.route("/json_per_topics")
def json7():
    return list_resume_claims_per_topics()

@app.route("/json_per_entity_date1_date2")
def json8():
    return list_resume_borne_date1_date2_entity()

if __name__ == '__main__':
    app.run(debug=True)
