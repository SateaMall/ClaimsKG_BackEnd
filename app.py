from flask import Flask, request
from markupsafe import Markup
from modules.dataframes import generate_global_dataframe
from modules.dataframes import generate_per_label_dataframe
from modules.statistics.summary import extract_topics, json_entity_topic_dates_searchs, json_topic_dates_searchs
from modules.statistics.summary import json_per_source_label, suggestions
from modules.statistics.summary import dico_numbers_resume
from flask_cors import CORS
from modules.statistics.summary import json_per_source_label
from modules.statistics.summary import json_per_entity

from modules.statistics.summary import json_number_false
from modules.statistics.summary import json_number_true
from modules.statistics.summary import json_number_mixture
from modules.statistics.summary import json_number_other
from modules.statistics.summary import list_resume_entite_per_label_filtre_per_date
from modules.statistics.summary import moy_ent_per_claims_for_df 
from modules.statistics.summary import list_resume_entite_per_langue_filtre_per_date
from modules.statistics.summary import json_per_source_label_true
from modules.statistics.summary import json_per_source_label_false
from modules.statistics.summary import json_per_source_label_mixture
from modules.statistics.summary import json_per_source_label_other
from modules.statistics.summary import list_resume_born_per_date_label
from modules.statistics.summary import json_entity_dates_searchs
from modules.statistics.summary import list_resume_claims_per_source_label
from modules.statistics.summary import list_resume_borne_date1_date2
from modules.statistics.summary import list_resume_borne_source
from modules.statistics.summary import list_resume_claims_per_topics2
from modules.statistics.summary import list_resume_claims_per_topics
from modules.statistics.summary import entite_per_source_filtre_per_date
from modules.statistics.summary import list_resume_borne_date1_date2_entity
from modules.statistics.summary import list_resume_claims_per_langues
from modules.statistics.summary import entity2
from modules.statistics.summary import list_resume_born_topics
from modules.statistics.summary import list_resume_entite_per_source_filtre_per_date
from modules.statistics.summary import list_resume_born_source_label

from modules.statistics.summary import dico_numbers_resume
from flask_cors import CORS
from modules.statistics.summary import list_resume_claims_per_date_label
from modules.statistics.summary import entity

app = Flask(__name__)
CORS(app)

############################Telecharger.csv###############################

@app.route ("/update_label_df")
def update_label_df():
    return generate_per_label_dataframe()

@app.route ("/update_global_df")
def update_global_df():
    return generate_global_dataframe()

##########################################################################

@app.route ("/graph/perlabeldate")
def graph_per_label_date():
    return 5

################################FONCTION SUMMARY############################

#### a enlever
# @app.route("/json_per_true")
# def json4():
#     return json_per_source_label_true()

# @app.route("/json_per_false")
# def json5():
#     return json_per_source_label_false()

# @app.route("/json_per_mixture")
# def json6():
#     return json_per_source_label_mixture()

# @app.route("/json_per_other")
# def json7():
#     return json_per_source_label_other()

@app.route("/number_false")
@app.route("/number_false/<date1>/<date2>")
def json_number_false_end(date1=None, date2=None):
    return json_number_false(date1, date2)

@app.route("/number_true")
@app.route("/number_true/<date1>/<date2>")
def json_number_true_end(date1=None, date2=None):
    return json_number_true(date1, date2)

@app.route("/number_mixture")
@app.route("/number_mixture/<date1>/<date2>")
def json_number_mixture_end(date1= None, date2=None):
    return json_number_mixture(date1, date2)

@app.route("/number_other")
@app.route("/number_other/<date1>/<date2>")
def json_number_other_end(date1=None, date2=None):
    return json_number_other(date1, date2)

###########################################################################



##################################FONCTION GRAPHE##########################

@app.route("/json_per_source_label")
def json_per_source_label():
    return list_resume_claims_per_source_label()

@app.route("/json_per_date1_label")
@app.route("/json_per_date1_label/<date1>/<date2>")
@app.route("/json_per_date1_label/<date1>/<date2>/<granularite>")
def json_filter_date(date1=None, date2=None, granularite=None):
    return list_resume_born_per_date_label(date1, date2, granularite)

@app.route("/json_per_date1_label")  ####a enlever 
def json_per_date1_date2():
    return list_resume_claims_per_date_label()

@app.route("/json_per_entity")
def json_entity():
    return entity()

@app.route("/entity2")
def entity():
    return entity2()

@app.route("/json_born_date/<date1>/<date2>")  #####supp ca
def json_borned_bydate(date1,date2):
    return list_resume_borne_date1_date2(date1,date2)

@app.route("/json_born_source/<source>")
def json6_borned_source(source):
    return list_resume_borne_source(source)

@app.route("/json_per_topics")
def json_topics():
    return list_resume_claims_per_topics2()

@app.route("/json_per_entity_date1_date2/<date1>/<date2>")
def json_born_entity_date(date1, date2):
    return list_resume_borne_date1_date2_entity(date1, date2)

@app.route("/json_per_langue_label")
@app.route("/json_per_langue_label/<date1>/<date2>")
def json_langue_label(date1=None, date2=None):
    return list_resume_claims_per_langues(date1, date2)


@app.route("/json_born_per_source_label/<date1>/<date2>")
def born_per_source_label(date1, date2):
    return list_resume_born_source_label(date1, date2)

@app.route("/json_born_per_topics/<date1>/<date2>")
def born_per_topics(date1, date2):
    return list_resume_born_topics(date1, date2)

########################################################   Endpoint Recherche  ###########################################################

@app.route("/json_born_entite_label_filtre_date")
@app.route("/json_born_entite_label_filtre_date/<entity>")
@app.route("/json_born_entite_label_filtre_date/<label>")
@app.route("/json_born_entite_label_filtre_date/<date1>/<date2>")
@app.route("/json_born_entite_label_filtre_date/<label>/<date1>/<date2>")
@app.route("/json_born_entite_label_filtre_date/<entity>/<date1>/<date2>")
@app.route("/json_born_entite_label_filtre_date/<entity>/<label>/<date1>/<date2>")
def born_per_entite_label_filtre_date(entity= None, label= None, date1= None, date2= None):
    if entity is not None:
        list_entity = entity.split(',')
    else:
        list_entity = [] 
    return list_resume_entite_per_label_filtre_per_date(list_entity, label, date1, date2)

@app.route("/json_born_entite_langue_filtre_date")
@app.route("/json_born_entite_langue_filtre_date/<entity>")
@app.route("/json_born_entite_langue_filtre_date/<langue>")
@app.route("/json_born_entite_langue_filtre_date/<date1>/<date2>")
@app.route("/json_born_entite_langue_filtre_date/<langue>/<date1>/<date2>")
@app.route("/json_born_entite_langue_filtre_date/<entity>/<date1>/<date2>")
@app.route("/json_born_entite_langue_filtre_date/<entity>/<langue>/<date1>/<date2>")
def born_per_entite_langue_filtre_date(entity=None, langue=None, date1= None, date2= None):
    if entity is not None:
        list_entity = entity.split(',')
    else:
        list_entity = [] 
    return list_resume_entite_per_langue_filtre_per_date(list_entity, langue, date1, date2)

@app.route("/json_born_entite_source_filtre_date")
@app.route("/json_born_entite_source_filtre_date/<entity>")
@app.route("/json_born_entite_source_filtre_date/<source>")
@app.route("/json_born_entite_source_filtre_date/<date1>/<date2>")
@app.route("/json_born_entite_source_filtre_date/<source>/<date1>/<date2>")
@app.route("/json_born_entite_source_filtre_date/<entity>/<date1>/<date2>")
@app.route("/json_born_entite_source_filtre_date/<entity>/<source>/<date1>/<date2>")
def born_per_entite_source_filtre_date(entity=None, source=None, date1= None, date2= None):
    if entity is not None:
        list_entity = entity.split(',')
    else:
        list_entity = [] 
    return list_resume_entite_per_source_filtre_per_date(list_entity, source, date1, date2)

##################################################################################################################

@app.route ("/resume")
def resume():
    return dico_numbers_resume()

### Extract topics 
@app.route("/topics")
def topics():
    return extract_topics()

### get suggestions for entity search auto completion
@app.route('/suggestions', methods=['GET'])
def suggestions_entity():
    query = request.args.get('query')
    return suggestions(query)

### Infos for the search form 
@app.route('/search-entity-topic', methods=['GET'])
def search_entity_topic():
    selectedEntities = request.args.getlist('selectedEntities')
    firstDate = request.args.get('firstDate')
    lastDate = request.args.get('lastDate')
    topic = request.args.get('topic')
    return json_entity_topic_dates_searchs(selectedEntities,firstDate,lastDate,topic)

### Infos for the search form 
@app.route('/search-entity', methods=['GET'])
def search_entity():
    selectedEntities = request.args.getlist('selectedEntities')
    firstDate = request.args.get('firstDate')
    lastDate = request.args.get('lastDate')
    return json_entity_dates_searchs(selectedEntities,firstDate,lastDate)

### Infos for the search form 
@app.route('/search-topic', methods=['GET'])
def search_topic():
    topic = request.args.get('topic')
    firstDate = request.args.get('firstDate')
    lastDate = request.args.get('lastDate')
    return json_topic_dates_searchs(firstDate, lastDate, topic)

if __name__ == '__main__':
    app.run(debug=True)










