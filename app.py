from flask import Flask, jsonify, request
import pandas
from flask_cors import CORS
from modules.dataframes import generate_dataframes
from modules.functions.dashboard import create_graph_data, top_categories_separated
from modules.functions.search_compare import search_entity_graph, search_language_graph, search_label_graph, search_source_graph, suggestions,  search_date_graph, suggestionsEntityTopic, extract_topics, filter_data_entity, filter_data_topic
from modules.functions.statistics import dico_numbers_resume
from modules.functions.statistics import json_number_false, json_number_other, json_number_true,  json_number_mixture
from modules.functions.dashboard import list_resume_born_per_date_label
from modules.functions.dashboard import list_resume_claims_per_source_label
from modules.functions.dashboard import list_resume_borne_date1_date2_entity
from modules.functions.dashboard import list_resume_claims_per_langues
from modules.functions.dashboard import list_resume_born_topics
from modules.functions.dashboard import list_resume_born_source_label
from modules.functions.dashboard import entity, entity2
from modules.dataframes.dataframe_singleton import df_simple
from modules.dataframes.dataframe_singleton import df_keyword
from modules.dataframes.dataframe_singleton import df_entity
from modules.dataframes.dataframe_singleton import df_topic


app = Flask(__name__)
CORS(app)

############################   DOWNLOAD.csv   ###############################


@app.route ("/update_dataframes")
def update_dataframes():
    return generate_dataframes()

################################    SUMMARY FUNCTION   ############################

#summary dashboard function
@app.route("/number_false")
@app.route("/number_false/<date1>/<date2>")
@app.route("/number_false/<entity>/<date1>/<date2>")
def json_number_false_end(date1=None, date2=None):
    return json_number_false(date1, date2)

#summary dashboard function
@app.route("/number_true")
@app.route("/number_true/<date1>/<date2>")
@app.route("/number_true/<entity>/<date1>/<date2>")
def json_number_true_end(date1=None, date2=None):
    return json_number_true(date1, date2)

#summary dashboard function
@app.route("/number_mixture")
@app.route("/number_mixture/<date1>/<date2>")
@app.route("/number_mixture/<entity>/<date1>/<date2>")
def json_number_mixture_end(date1= None, date2=None):
    return json_number_mixture(date1, date2)

#summary dashboard function
@app.route("/number_other")
@app.route("/number_other/<date1>/<date2>")
@app.route("/number_other/<entity>/<date1>/<date2>")
def json_number_other_end(date1=None, date2=None):
    return json_number_other(date1, date2)

###########################################################################



##################################    DASHBOARD GRAPH FUNCTION    ##########################

#third dashboard graph function
@app.route("/json_per_source_label")
def json_per_source_label():
    return list_resume_claims_per_source_label()

#first dashboard graph function with filtered
@app.route("/json_per_date1_label",methods=['GET'])
def json_filter_date(date1=None, date2=None, granularite=None):
    date1 = request.args.get('date1')
    date2 = request.args.get('date2')
    granularite = request.args.get('granularite')
    return list_resume_born_per_date_label(date1, date2, granularite)

#second dashboard graph function
@app.route("/json_per_entity")
def json_entity():
    return entity()

# sert mais je ne sais pas à quoi
@app.route("/entity2")
def entity():
    return entity2()

#second dashboard graph function with filtered
@app.route("/json_per_entity_date1_date2")
@app.route("/json_per_entity_date1_date2/<date1>/<date2>")
def json_born_entity_date(date1=None, date2=None):
    return list_resume_borne_date1_date2_entity(date1, date2)

#fourth dashboard graph function with filtered
@app.route("/json_per_langue_label")
@app.route("/json_per_langue_label/<date1>/<date2>")
def json_langue_label(date1=None, date2=None):
    return list_resume_claims_per_langues(date1, date2)

#third dashboard graph function with filtered
@app.route("/json_born_per_source_label/<date1>/<date2>")
def born_per_source_label(date1=None, date2=None):
    return list_resume_born_source_label(date1, date2)

#sixth dashboard graph function with filtered
@app.route("/json_born_per_topics")
@app.route("/json_born_per_topics/<date1>/<date2>")
def born_per_topics(date1=None, date2=None):
    data = list_resume_born_topics(date1, date2)
    return jsonify(data)


#################################################   SEARCH PART    #############################################################

@app.route('/topics-by-quantity')
def top_categories():
    return jsonify(top_categories_separated())

@app.route('/graph-data')
def graph_data():
    nodes, edges = create_graph_data()
    return jsonify({"nodes": nodes, "edges": edges})

# home page function
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

### Search part first graph 
@app.route('/search-entity1', methods=['GET'])
def search_entity1():
    selectedEntities = request.args.getlist('selectedEntities')
    firstDate = request.args.get('firstDate')
    lastDate = request.args.get('lastDate')
    filtered_df_entity = filter_data_entity(df_entity, selectedEntities, firstDate, lastDate)
    return search_date_graph(filtered_df_entity,  selectedEntities = selectedEntities)
 

### Search form (Entity)
@app.route('/search-entity2', methods=['GET'])
def search_entity2():
    selectedEntities = request.args.getlist('selectedEntities')
    firstDate = request.args.get('firstDate')
    lastDate = request.args.get('lastDate')
    filtered_df = filter_data_entity(df_entity, selectedEntities, firstDate, lastDate)
    return search_label_graph(filtered_df)

@app.route('/search-entity3', methods=['GET'])
def search_entity3():
    selectedEntities = request.args.getlist('selectedEntities')
    firstDate = request.args.get('firstDate')
    lastDate = request.args.get('lastDate')
    filtered_df = filter_data_entity(df_entity, selectedEntities, firstDate, lastDate)
    return search_source_graph(filtered_df)

@app.route('/search-entity4', methods=['GET'])
def search_entity4():
    selectedEntities = request.args.getlist('selectedEntities')
    firstDate = request.args.get('firstDate')
    lastDate = request.args.get('lastDate')
    filtered_df = filter_data_entity(df_entity, selectedEntities, firstDate, lastDate)
    return search_language_graph(filtered_df)

### ### Search form (Topic)
@app.route('/search-topic1', methods=['GET'])
def search_topic1():
    topic = request.args.get('topic')
    firstDate = request.args.get('firstDate')
    lastDate = request.args.get('lastDate')
    filtered_df = filter_data_topic(topic, firstDate, lastDate)
    merged_df = pandas.merge(filtered_df, df_keyword[['claimReview_url','id2']], on='claimReview_url', how='left')
    return search_date_graph(merged_df, topic=topic)

@app.route('/search-topic2', methods=['GET'])
def search_topic2():
    topic = request.args.get('topic')
    firstDate = request.args.get('firstDate')
    lastDate = request.args.get('lastDate')
    filtered_df = filter_data_topic(topic, firstDate, lastDate)
    return search_label_graph(filtered_df)

@app.route('/search-topic3', methods=['GET'])
def search_topic3():
    topic = request.args.get('topic')
    firstDate = request.args.get('firstDate')
    lastDate = request.args.get('lastDate')
    filtered_df = filter_data_topic(topic, firstDate, lastDate)
    return search_source_graph(filtered_df)

@app.route('/search-topic4', methods=['GET'])
def search_topic4():
    topic = request.args.get('topic')
    firstDate = request.args.get('firstDate')
    lastDate = request.args.get('lastDate')
    filtered_df = filter_data_topic(topic, firstDate, lastDate)
    return search_entity_graph(filtered_df)

### ### Search form (Topic-Entity)
@app.route('/search-topic-entity1', methods=['GET'])
def search_entity_topic1():
    selectedEntities = request.args.getlist('selectedEntities')
    firstDate = request.args.get('firstDate')
    lastDate = request.args.get('lastDate')
    topic = request.args.get('topic')
    filtered_df_topic = filter_data_topic(topic, firstDate, lastDate)
    filtered_df_topic['id1'] = filtered_df_topic['claimReview_url']
    filtered_df_entity_topic = filter_data_entity(filtered_df_topic, selectedEntities, firstDate, lastDate)
    ## Link topic with keywords
    merged_df = pandas.merge(filtered_df_entity_topic, df_keyword[['claimReview_url','id2']], on='claimReview_url', how='left')
    return search_date_graph(merged_df, selectedEntities = selectedEntities,topic = topic)



@app.route('/search-topic-entity2', methods=['GET'])
def search_entity_topic2():
    selectedEntities = request.args.getlist('selectedEntities')
    firstDate = request.args.get('firstDate')
    lastDate = request.args.get('lastDate')
    topic = request.args.get('topic')
    filtered_df_topic = filter_data_topic(topic, firstDate, lastDate)
    filtered_df_topic['id1'] = filtered_df_topic['claimReview_url']
    filtered_df = filter_data_entity(filtered_df_topic, selectedEntities, firstDate, lastDate)
    return search_label_graph(filtered_df)


@app.route('/search-topic-entity3', methods=['GET'])
def search_entity_topic3():
    selectedEntities = request.args.getlist('selectedEntities')
    firstDate = request.args.get('firstDate')
    lastDate = request.args.get('lastDate')
    topic = request.args.get('topic')
    filtered_df_topic = filter_data_topic(topic, firstDate, lastDate)
    filtered_df_topic['id1'] = filtered_df_topic['claimReview_url']
    filtered_df = filter_data_entity(filtered_df_topic, selectedEntities, firstDate, lastDate)
    return search_source_graph(filtered_df)


if __name__ == '__main__':
    app.run(debug=True)










