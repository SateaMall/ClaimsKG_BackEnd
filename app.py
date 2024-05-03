from flask import Flask, request
from modules.dataframes import generate_global_dataframe
from modules.dataframes import generate_per_label_dataframe
from modules.statistics.summary import json_per_date1_label
from modules.statistics.summary import json_per_source_label, suggestions
from modules.statistics.summary import dico_numbers_resume
from flask_cors import CORS
from modules.statistics.summary import json_per_source_label
from modules.statistics.summary import json_per_entity
from modules.statistics.summary import json_number_false
from modules.statistics.summary import json_number_true
from modules.statistics.summary import json_number_mixture
from modules.statistics.summary import json_number_other
from modules.statistics.summary import moy_ent_per_claims_for_df 
from modules.statistics.summary import json_per_source_label_true
from modules.statistics.summary import json_per_source_label_false
from modules.statistics.summary import json_per_source_label_mixture
from modules.statistics.summary import json_per_source_label_other
from modules.statistics.summary import json_entity_dates_searchs
from modules.dataframes.dataframe_singleton import df_complete
from modules.statistics.summary import list_resume_claims_per_source_label
from modules.statistics.summary import list_resume_borne_date1_date2
from modules.statistics.summary import list_resume_borne_entities
from modules.statistics.summary import list_resume_borne_source

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

@app.route ("/graph/perlabeldate")
def graph_per_label_date():
    return 5

@app.route ("/resume")
def resume():
    return dico_numbers_resume()

@app.route("/json_per_source_label")
def json():
    return list_resume_claims_per_source_label()

@app.route("/json_per_date1_label/<date1>/<date2>")
def json2(date1,date2):
    return json_per_date1_label(date1,date2)

@app.route("/json_per_entity")
def json3():
    return json_per_entity()
g
@app.route("/json_per_true")
def json4():
    return json_per_source_label_true()

@app.route("/json_per_false")
def json5():
    return json_per_source_label_false()

@app.route("/json_per_mixture")
def json6():
    return json_per_source_label_mixture()

@app.route("/json_per_other")
def json7():
    return json_per_source_label_other()

@app.route("/number_false")
def json_number_false_end():
    return json_number_false()

@app.route("/number_true")
def json_number_true_end():
    return json_number_true()

@app.route("/number_mixture")
def json_number_mixture_end():
    return json_number_mixture()

@app.route("/number_other")
def json_number_other_end():
    return json_number_other()

@app.route('/suggestions', methods=['GET'])
def suggestions_entity():
    query = request.args.get('query')
    return suggestions(query)

@app.route('/search', methods=['GET'])
def search():
    selectedEntities = request.args.get('selectedEntities')
    firstDate = request.args.get('firstDate')
    lastDate = request.args.get('lastDate')
    return json_entity_dates_searchs(selectedEntities,firstDate,lastDate)
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

if __name__ == '__main__':
    app.run(debug=True)