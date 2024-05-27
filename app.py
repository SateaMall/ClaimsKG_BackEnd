from flask import Flask, jsonify, request
import langcodes
from markupsafe import Markup
from modules.dataframes import generate_global_dataframe
from modules.dataframes import generate_per_label_dataframe
from modules.statistics.summary import common_categories, create_graph_data, extract_topics, filter_data_entity, filter_data_topic, filter_data_topic_entity, suggestionsEntityTopic
from modules.statistics.summary import suggestions
from modules.statistics.summary import dico_numbers_resume
from flask_cors import CORS
from modules.statistics.summary import json_number_false
from modules.statistics.summary import json_number_true
from modules.statistics.summary import json_number_mixture
from modules.statistics.summary import json_number_other
from modules.statistics.summary import list_resume_entite_per_label_filtre_per_date
from modules.statistics.summary import list_resume_entite_per_langue_filtre_per_date
from modules.statistics.summary import list_resume_born_per_date_label
from modules.statistics.summary import list_resume_claims_per_source_label
from modules.statistics.summary import list_resume_borne_source
from modules.statistics.summary import list_resume_claims_per_topics2
from modules.statistics.summary import list_resume_borne_date1_date2_entity
from modules.statistics.summary import list_resume_claims_per_langues
from modules.statistics.summary import entity2
from modules.statistics.summary import list_resume_born_topics
from modules.statistics.summary import list_resume_entite_per_source_filtre_per_date
from modules.statistics.summary import list_resume_born_source_label
from modules.statistics.summary import dico_numbers_resume
from flask_cors import CORS
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

################################FONCTION SUMMARY############################

#fonction summary dashboard
@app.route("/number_false")
@app.route("/number_false/<date1>/<date2>")
def json_number_false_end(date1=None, date2=None):
    return json_number_false(date1, date2)

#fonction summary dashboard
@app.route("/number_true")
@app.route("/number_true/<date1>/<date2>")
def json_number_true_end(date1=None, date2=None):
    return json_number_true(date1, date2)

#fonction summary dashboard
@app.route("/number_mixture")
@app.route("/number_mixture/<date1>/<date2>")
def json_number_mixture_end(date1= None, date2=None):
    return json_number_mixture(date1, date2)

#fonction summary dashboard
@app.route("/number_other")
@app.route("/number_other/<date1>/<date2>")
def json_number_other_end(date1=None, date2=None):
    return json_number_other(date1, date2)

###########################################################################



##################################FONCTION GRAPHE##########################

#fonction troisieme graphe dashboard
@app.route("/json_per_source_label")
def json_per_source_label():
    return list_resume_claims_per_source_label()

#fonction du premier graphe dashboard
@app.route("/json_per_date1_label")
@app.route("/json_per_date1_label/<date1>/<date2>")
@app.route("/json_per_date1_label/<date1>/<date2>/<granularite>")
def json_filter_date(date1=None, date2=None, granularite=None):
    return list_resume_born_per_date_label(date1, date2, granularite)

#fonction deuxieme graphe dashboard
@app.route("/json_per_entity")
def json_entity():
    return entity()

@app.route("/entity2")
def entity():
    return entity2()

@app.route("/json_born_source/<source>")
def json6_borned_source(source):
    return list_resume_borne_source(source)

@app.route("/json_per_topics")
def json_topics():
    return list_resume_claims_per_topics2()

########################################## ayoub ################################

@app.route("/json_per_entity_date1_date2")
@app.route("/json_per_entity_date1_date2/<entity>")
@app.route("/json_per_entity_date1_date2/<date1>/<date2>")
@app.route("/json_per_entity_date1_date2/<entity>/<date1>/<date2>")
def json_born_entity_date(entity=None, date1=None, date2=None):
    if entity is not None:
        list_entity = entity.split(',')
    else:
        list_entity = [] 
    return list_resume_borne_date1_date2_entity(list_entity, date1, date2)

################################################################################

#fonction quatrieme graphe dashbord
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
@app.route('/top-topics')
def top_categories():
    return jsonify(common_categories())

@app.route('/graph-data')
def graph_data():
    nodes, edges = create_graph_data()
    return jsonify({"nodes": nodes, "edges": edges})




# fonction home page
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

### get suggestions for entity-topic search auto completion
@app.route('/suggestions-topic', methods=['GET'])
def suggestions_entity_topic():
    query = request.args.get('query')
    topic = request.args.get('topic')
    return suggestionsEntityTopic(query,topic)



# graphe partie recherche 
@app.route('/search-entity1', methods=['GET'])
def search_entity1():
    selectedEntities = request.args.getlist('selectedEntities')
    firstDate = request.args.get('firstDate')
    lastDate = request.args.get('lastDate')
    filtered_df = filter_data_entity(selectedEntities, firstDate, lastDate)
    grouped_df = filtered_df.groupby(['date1', 'label']).size().reset_index(name='counts')
    grouped_df['date1'] = grouped_df['date1'].astype(str)
    data = grouped_df.to_dict(orient='records')
    return jsonify(data)


### Search form (Entity)
@app.route('/search-entity2', methods=['GET'])
def search_entity2():
    selectedEntities = request.args.getlist('selectedEntities')
    firstDate = request.args.get('firstDate')
    lastDate = request.args.get('lastDate')
    filtered_df = filter_data_entity(selectedEntities, firstDate, lastDate)
    grouped_df = filtered_df.groupby('label').size().reset_index(name='counts')
    data = grouped_df.to_dict(orient='records')
    return jsonify(data)

@app.route('/search-entity3', methods=['GET'])
def search_entity3():
    selectedEntities = request.args.getlist('selectedEntities')
    firstDate = request.args.get('firstDate')
    lastDate = request.args.get('lastDate')
    filtered_df = filter_data_entity(selectedEntities, firstDate, lastDate)
    grouped_df = filtered_df.groupby(['source', 'label']).size().reset_index(name='counts')
    data = grouped_df.to_dict(orient='records')
    return jsonify(data)

@app.route('/search-entity4', methods=['GET'])
def search_entity4():
    selectedEntities = request.args.getlist('selectedEntities')
    firstDate = request.args.get('firstDate')
    lastDate = request.args.get('lastDate')
    filtered_df = filter_data_entity(selectedEntities, firstDate, lastDate)
     # Convert language codes to full language names
    filtered_df['reviewBodyLang'] = filtered_df['reviewBodyLang'].apply(convert_lang_code_to_name)
    grouped_df = filtered_df.groupby(['reviewBodyLang', 'label']).size().reset_index(name='counts')
    data = grouped_df.to_dict(orient='records')
    return jsonify(data)

def convert_lang_code_to_name(lang_code):
    try:
        return langcodes.Language.get(lang_code).display_name()
    except:
        return lang_code



### ### Search form (Topic)
@app.route('/search-topic1', methods=['GET'])
def search_topic1():
    topic = request.args.get('topic')
    firstDate = request.args.get('firstDate')
    lastDate = request.args.get('lastDate')
    filtered_df = filter_data_topic(topic, firstDate, lastDate)
    grouped_df = filtered_df.groupby(['date1', 'label']).size().reset_index(name='counts')
    grouped_df['date1'] = grouped_df['date1'].astype(str)
    grouped_df['label'] = grouped_df['label'].str.upper()
    grouped_df['total'] = grouped_df.groupby('date1')['counts'].transform('sum')
    data = grouped_df.to_dict(orient='records')
    return jsonify(data)


@app.route('/search-topic2', methods=['GET'])
def search_topic2():
    topic = request.args.get('topic')
    firstDate = request.args.get('firstDate')
    lastDate = request.args.get('lastDate')
    filtered_df = filter_data_topic(topic, firstDate, lastDate)
    grouped_df = filtered_df.groupby('label').size().reset_index(name='counts')
    grouped_df['label'] = grouped_df['label'].str.upper()
    data = grouped_df.to_dict(orient='records')
    return jsonify(data)

@app.route('/search-topic3', methods=['GET'])
def search_topic3():
    topic = request.args.get('topic')
    firstDate = request.args.get('firstDate')
    lastDate = request.args.get('lastDate')
    filtered_df = filter_data_topic(topic, firstDate, lastDate)
    grouped_df = filtered_df.groupby(['source', 'label']).size().reset_index(name='counts')
    grouped_df['label'] = grouped_df['label'].str.upper()
    data = grouped_df.to_dict(orient='records')
    return jsonify(data)

@app.route('/search-topic4', methods=['GET'])
def search_topic4():
    topic = request.args.get('topic')
    firstDate = request.args.get('firstDate')
    lastDate = request.args.get('lastDate')
    filtered_df = filter_data_topic(topic, firstDate, lastDate)
  # Group by entity and count occurrences, then get the top 50 entities
    top_entities_df = (
        filtered_df['entity']
        .value_counts()
        .reset_index(name='counts')
        .rename(columns={'index': 'entity'})
        .head(50)
    )

    data = top_entities_df.to_dict(orient='records')
    return jsonify(data)




### ### Search form (Topic-Entity)
@app.route('/search-topic-entity1', methods=['GET'])
def search_entity_topic1():
    selectedEntities = request.args.getlist('selectedEntities')
    firstDate = request.args.get('firstDate')
    lastDate = request.args.get('lastDate')
    topic = request.args.get('topic')
    filtered_df = filter_data_topic_entity(selectedEntities, topic, firstDate, lastDate)
    grouped_df = filtered_df.groupby(['date1', 'label']).size().reset_index(name='counts')
    grouped_df['date1'] = grouped_df['date1'].astype(str)
    grouped_df['label'] = grouped_df['label'].str.upper()
    grouped_df['total'] = grouped_df.groupby('date1')['counts'].transform('sum')
    data = grouped_df.to_dict(orient='records')
    return jsonify(data)


@app.route('/search-topic-entity2', methods=['GET'])
def search_entity_topic2():
    selectedEntities = request.args.getlist('selectedEntities')
    firstDate = request.args.get('firstDate')
    lastDate = request.args.get('lastDate')
    topic = request.args.get('topic')
    filtered_df = filter_data_topic_entity(selectedEntities, topic, firstDate, lastDate)
    grouped_df = filtered_df.groupby('label').size().reset_index(name='counts')
    grouped_df['label'] = grouped_df['label'].str.upper()
    data = grouped_df.to_dict(orient='records')

    return jsonify(data)


@app.route('/search-topic-entity3', methods=['GET'])
def search_entity_topic3():
    selectedEntities = request.args.getlist('selectedEntities')
    firstDate = request.args.get('firstDate')
    lastDate = request.args.get('lastDate')
    topic = request.args.get('topic')
    filtered_df = filter_data_topic_entity(selectedEntities, topic, firstDate, lastDate)
    grouped_df = filtered_df.groupby(['source', 'label']).size().reset_index(name='counts')
    grouped_df['label'] = grouped_df['label'].str.upper()
    data = grouped_df.to_dict(orient='records')

    print (jsonify(data))
    return jsonify(data)


if __name__ == '__main__':
    app.run(debug=True)










