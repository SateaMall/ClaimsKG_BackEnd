from collections import defaultdict
import json
from typing import Counter

from flask import current_app, jsonify
import pandas

from langcodes import Language
from itertools import combinations
from modules.dataframes.dataframe_singleton import df_complete
from modules.dataframes.dataframe_singleton import df_other
from modules.dataframes.dataframe_singleton import df_Source_labelFALSE
from modules.dataframes.dataframe_singleton import df_Source_labelMIXTURE
from modules.dataframes.dataframe_singleton import df_Source_labelOTHER
from modules.dataframes.dataframe_singleton import df_Source_labelTRUE



#################################################   HOME page function   #########################################

def claims_total():
    nb_cw_total = len(df_complete['id2'].unique())
    nb_cr_total = len(df_complete['id1'].unique())
    return nb_cw_total, nb_cr_total


def total_claim_review():
    nb_cr_total = len(df_complete['id1'].unique())
    return nb_cr_total

def get_dates():
    min1 = pandas.to_datetime(df_complete['date1'].dropna(), errors='coerce').min()
    max1 = pandas.to_datetime(df_complete['date1'].dropna(), errors='coerce').max()
    min1 = min1.strftime('%B %d, %Y')
    max1 = max1.strftime('%B %d, %Y')
    return min1, max1

def numbers_of_entities():
    nb_entities = len(df_complete['entity'].dropna().unique())
    return nb_entities


############################################################   GRAPH FUNCTION WITHOUT PARAMETER  #####################################################################

#third graph function
def claims_per_source_label():
    filtre = df_complete['source'].notna()
    df_filtre = df_complete[filtre]
    filtre2 = df_filtre['label'].notna() 
    df_filtre2 = df_filtre[filtre2]
    filtre_group_notna = df_filtre2.groupby(['id1','source','label'])['source'].size().reset_index(name='counts')
    final_grouped = filtre_group_notna.groupby(['source', 'label'])['counts'].size().reset_index(name='counts')

    return final_grouped


#second graph function
def number_entity():
    filtre = df_complete['entity'].notna()
    df_filtre = df_complete[filtre] 
    filtre_group_notna = df_filtre.groupby(['entity'])['entity'].size().reset_index(name='counts')
    return filtre_group_notna

#sert mais je ne sais pas a quoi
def number_entity2():
    filtre = df_complete['entity'].notna()
    df_filtre = df_complete[filtre]
    filtre_group_notna = df_filtre['entity'].value_counts().reset_index()
    filtre_group_notna.columns = ['entity', 'counts']
    filtre_group_notna = filtre_group_notna.sort_values('counts', ascending=False).head(50)

    return filtre_group_notna



######################################################################  SUMMARY FUNCTION  ###########################################################################


def number_label_false(entity, date1, date2):
    df_filtre = df_complete
    df_filtre = df_filtre[(df_filtre['label'] == 'FALSE')]

    if entity != []:
        if isinstance(entity, list):
            df_filtre = df_filtre[df_filtre['entity'].isin(entity)]
        else:
            df_filtre = df_filtre[(df_filtre['entity'] == entity)]     
    if date1 is not None: 
        df_filtre = df_filtre[(df_filtre['date1'] >= date1) & (df_filtre['date1'] <= date2)]

    nb_cw_total = len(df_filtre['id1'].unique())
    return nb_cw_total

def number_label_true(entity, date1, date2):
    df_filtre = df_complete
    df_filtre = df_filtre[(df_filtre['label'] == 'TRUE')]

    if entity != []:
        if isinstance(entity, list):
            df_filtre = df_filtre[df_filtre['entity'].isin(entity)]
        else:
            df_filtre = df_filtre[(df_filtre['entity'] == entity)]     
    if date1 is not None: 
        df_filtre = df_filtre[(df_filtre['date1'] >= date1) & (df_filtre['date1'] <= date2)]

    nb_cw_total = len(df_filtre['id1'].unique())
    return nb_cw_total


def number_label_mixture(entity, date1, date2):
    df_filtre = df_complete
    df_filtre = df_filtre[(df_filtre['label'] == 'MIXTURE')]

    if entity != []:
        if isinstance(entity, list):
            df_filtre = df_filtre[df_filtre['entity'].isin(entity)]
        else:
            df_filtre = df_filtre[(df_filtre['entity'] == entity)]     
    if date1 is not None: 
        df_filtre = df_filtre[(df_filtre['date1'] >= date1) & (df_filtre['date1'] <= date2)]

    nb_cw_total = len(df_filtre['id1'].unique())
    return nb_cw_total

def number_label_other(entity, date1, date2):
    df_filtre = df_complete
    df_filtre = df_filtre[(df_filtre['label'] == 'OTHER')]

    if entity != []:
        if isinstance(entity, list):
            df_filtre = df_filtre[df_filtre['entity'].isin(entity)]
        else:
            df_filtre = df_filtre[(df_filtre['entity'] == entity)]     
    if date1 is not None: 
        df_filtre = df_filtre[(df_filtre['date1'] >= date1) & (df_filtre['date1'] <= date2)]

    nb_cw_total = len(df_filtre['id1'].unique())
    return nb_cw_total



##############################################################  FUNCTION GRAPH WITH PARAMETER   #############################################################################

#second dashboard graph function with filtered
def borne_entity(entity, dat1, dat2):
    filtre = df_complete['entity'].notna()
    df_filtre = df_complete[filtre]

    if entity != []:
        if isinstance(entity, list):
            df_filtre = df_filtre[~df_filtre['entity'].isin(entity)]
        else:
            df_filtre = df_filtre[(df_filtre['entity'] != entity)]
    if dat1 is not None:    
        df_filtre = df_filtre[(df_filtre['date1'] >= dat1) & (df_filtre['date1'] <= dat2)]

    filtre_group_notna = df_filtre.groupby(['entity'])['entity'].size().reset_index(name='counts')
    
    return filtre_group_notna.sort_values('counts', ascending=False).head(50)


#third dashboard graph function with filtered
def born_per_source_label(entity, dat1, dat2):
    filtre = df_complete['source'].notna()
    df_filtre = df_complete[filtre]
    filtre2 = df_filtre['label'].notna() 
    df_filtre2 = df_filtre[filtre2]
    filtre3 = df_filtre2['entity'].notna()
    df_filtre3 = df_filtre2[filtre3]
    filtre4 = df_filtre3['date1'].str.contains(r'^\d{4}-\d{2}-\d{2}$') & df_filtre3['date1'].notna()
    df_filtre3 = df_filtre3[filtre4]

    if entity != []:
        if isinstance(entity, list):
            df_filtre3 = df_filtre3[df_filtre3['entity'].isin(entity)]
        else:
            df_filtre3 = df_filtre3[(df_filtre3['entity'] == entity)]
    df_filtre3 = df_filtre3[(df_filtre3['date1'] >= dat1) & (df_filtre3['date1'] <= dat2)]

    filtre_group_notna = df_filtre3.groupby(['id1','source','label'])['source'].size().reset_index(name='counts')
    final_grouped = filtre_group_notna.groupby(['source', 'label'])['counts'].size().reset_index(name='counts')

    return final_grouped

#function to change format
def format_entity(entity):
    return f"http://dbpedia.org/resource/{entity.replace(' ', '_')}"


#sixth dashboard graph function with filtered
def born_per_topics_date(date1=None, date2=None):
    df_filtered = df_other.copy()
    if date1 and date2 is not None :
        df_filtered['date1'] = pandas.to_datetime(df_filtered['date1'])

        # Filter by date range
        mask_date = (df_filtered['date1'] >= date1) & (df_filtered['date1'] <= date2)
        df_filtered = df_filtered.loc[mask_date]

    return df_filtered

#first dashboard graph function with filtered
def born_per_date_label(entity, date1, date2, granularite):
    filtre = df_complete['date1'].str.contains(r'^\d{4}-\d{2}-\d{2}$') & df_complete['date1'].notna()
    df_filtre = df_complete[filtre]
    filtre2 = df_filtre['label'].notna() 
    df_filtre3 = df_filtre[filtre2]
    filtre3 = df_filtre3['entity'].notna()
    df_filtre3 = df_filtre3[filtre3]

    if entity != []:
        if isinstance(entity, list):
            df_filtre3 = df_filtre3[df_filtre3['entity'].isin(entity)]
        else:
            df_filtre3 = df_filtre3[(df_filtre3['entity'] == entity)]
    if date1 is not None :
        df_filtre3 = df_filtre3[(df_filtre['date1'] >= date1) & (df_filtre['date1'] <= date2)]
    if(granularite=="annee"):
        df_filtre3['date1'] = df_filtre3['date1'].str[:4]
    if(granularite == "mois") : 
        df_filtre3['date1'] = df_filtre3['date1'].str[:7] 
    
    filtre_group_notna = df_filtre3.groupby(['id1','date1','label'])['date1'].size().reset_index(name='counts')
    final_grouped = filtre_group_notna.groupby(['date1', 'label'])['counts'].size().reset_index(name='counts')


    return final_grouped

#fourth dashboard graph function with filtered
def langue_per_label(entity, dat1, dat2):

    filtre = df_complete['reviewBodyLang'].notna()
    df_filtre = df_complete[filtre] 
    filtre2 = df_filtre['label'].notna()
    df_filtre2 = df_filtre[filtre2]
    filtre3 = df_filtre2['date1'].str.contains(r'^\d{4}-\d{2}-\d{2}$') & df_filtre2['date1'].notna()
    df_filtre2 = df_filtre2[filtre3]
    filtre4 = df_filtre2['entity'].notna()
    df_filtre2 = df_filtre2[filtre4]

    if entity != []:
        if isinstance(entity, list):
            df_filtre2 = df_filtre2[df_filtre2['entity'].isin(entity)]
        else:
            df_filtre2 = df_filtre2[(df_filtre2['entity'] == entity)]
    if dat1 is not None: 
        df_filtre2 = df_filtre2[(df_filtre['date1'] >= dat1) & (df_filtre2['date1'] <= dat2)]

    filtre_group_notna = df_filtre2.groupby(['id1','reviewBodyLang', 'label'])['reviewBodyLang'].size().reset_index(name='counts')
    final_grouped = filtre_group_notna.groupby(['reviewBodyLang', 'label'])['counts'].size().reset_index(name='counts')

    return final_grouped




#################################################################    LANGUAGE CODE CHANGE FUNCTION    #######################################################

def changecode_langue(code):
    try:
        lang = Language.get(code)
        return lang.display_name()
    except ValueError:
        return code



#################################################################   FOR A FUTUR SEARCH PART FUNCTION   #########################################################################

def entite_per_label_filtre_per_date(entity, label, dat1, dat2):
    filtre = df_complete['label'].notna()
    df_filtre = df_complete[filtre]
    filtre2 = df_filtre['entity'].notna()
    df_filtre2 = df_filtre[filtre2]
    filtre3 = df_filtre2['date1'].str.contains(r'^\d{4}-\d{2}-\d{2}$') & df_filtre2['date1'].notna()
    df_filtre3 = df_filtre2[filtre3]
    if (label == "true"):
        label="TRUE"
    if (label == "false"):
        label="FALSE"
    if (label == "mixture"):
        label="MIXTURE"
    if (label == "other"):
        label="OTHER"    
 
    if entity is not None:
        if isinstance(entity, list):
            df_filtre3 = df_filtre3[df_filtre3['entity'].isin(entity)]
        else:
            df_filtre3 = df_filtre3[(df_filtre3['entity'] == entity)]
    if dat1 is not None: 
        df_filtre3 = df_filtre3[(df_filtre3['date1'] >= dat1) & (df_filtre3['date1'] <= dat2)]
    if label is not None: 
        df_filtre3 = df_filtre3[(df_filtre3['label'] == label)]

    filtre_group_notna = df_filtre3.groupby(['id1','entity', 'label'])['entity'].size().reset_index(name='counts')
    final_grouped = filtre_group_notna.groupby(['entity', 'label'])['counts'].size().reset_index(name='counts')

    return final_grouped


def entite_per_langue_filtre_per_date(entity, langue, dat1, dat2):
    filtre = df_complete['reviewBodyLang'].notna()
    df_filtre = df_complete[filtre] 
    filtre2 = df_filtre['entity'].notna()
    df_filtre2 = df_filtre[filtre2]
    filtre3 = df_filtre2['date1'].str.contains(r'^\d{4}-\d{2}-\d{2}$') & df_filtre2['date1'].notna()
    df_filtre3 = df_filtre2[filtre3]
    
    if entity is not None:
        if isinstance(entity, list):
            df_filtre3 = df_filtre3[df_filtre3['entity'].isin(entity)]
        else:
            df_filtre3 = df_filtre3[(df_filtre3['entity'] == entity)]
    if dat1 is not None: 
        df_filtre3 = df_filtre3[(df_filtre3['date1'] >= dat1) & (df_filtre3['date1'] <= dat2)]
    if langue is not None:
        df_filtre3 = df_filtre3[(df_filtre3['reviewBodyLang'] == langue)]
    
    filtre_group_notna = df_filtre3.groupby(['id1','entity', 'reviewBodyLang'])['entity'].size().reset_index(name='counts')
    final_grouped = filtre_group_notna.groupby(['entity', 'reviewBodyLang'])['counts'].size().reset_index(name='counts')

    return final_grouped


def entite_per_source_filtre_per_date(entity, source, dat1, dat2):
    filtre = df_complete['source'].notna()
    df_filtre = df_complete[filtre] 
    filtre2 = df_filtre['entity'].notna()
    df_filtre2 = df_filtre[filtre2]
    filtre3 = df_filtre2['date1'].str.contains(r'^\d{4}-\d{2}-\d{2}$') & df_filtre2['date1'].notna()
    df_filtre3 = df_filtre2[filtre3]

    if entity is not None:
        if isinstance(entity, list):
            df_filtre3 = df_filtre3[df_filtre3['entity'].isin(entity)]
        else:
            df_filtre3 = df_filtre3[(df_filtre3['entity'] == entity)]
    if dat1 is not None: 
        df_filtre3 = df_filtre3[(df_filtre3['date1'] >= dat1) & (df_filtre3['date1'] <= dat2)]
    if source is not None:
        df_filtre3 = df_filtre3[(df_filtre3['source'] == source)]


    filtre_group_notna = df_filtre3.groupby(['id1','entity', 'source'])['entity'].size().reset_index(name='counts')
    final_grouped = filtre_group_notna.groupby(['entity', 'source'])['counts'].size().reset_index(name='counts')
    
    return final_grouped




###################################################################################   JSON    ######################################################################################""


##########################################################################  JSON FOR A FUTUR SEARCH PART  ############################################################################### 


def list_resume_entite_per_label_filtre_per_date(entity, label, date1, date2):

    call_function = entite_per_label_filtre_per_date(entity, label, date1, date2)
    parsed_data = call_function.to_dict(orient='records')

    json_format = json.dumps(parsed_data)

    return json_format

def list_resume_entite_per_langue_filtre_per_date(entity, langue, date1, date2):

    call_function = entite_per_langue_filtre_per_date(entity, langue, date1, date2)
    parsed_data = call_function.to_dict(orient='records')
    for item in parsed_data:
        language_code = item['reviewBodyLang']  
        language_name = changecode_langue(language_code)
        item['reviewBodyLang'] = language_name

    json_format = json.dumps(parsed_data)

    return json_format

def list_resume_entite_per_source_filtre_per_date(entity, source, date1, date2):

    call_function = entite_per_source_filtre_per_date(entity, source, date1, date2)
    parsed_data = call_function.to_dict(orient='records')

    json_format = json.dumps(parsed_data)

    return json_format


######################################################################  JSON Home Page  #####################################

def dico_numbers_resume():
    total = claims_total()
    list = [
        {"Numbers of claims ": str(total[0]),
         "Numbers of claims review ": str(total_claim_review()),
         "Since ": str(get_dates()[0]), "to ": str(get_dates()[1]),
         "Numbers of entities ": str(numbers_of_entities()),
         }]

    list_json = json.dumps(list)
    return list_json


########################################################################## JSON FUNCTION WITHOUT PARAMETER  ############################################################################


# JSON third graph
def list_resume_claims_per_source_label():

    call_function = claims_per_source_label()
    parsed_data = call_function.to_dict(orient='records')

    json_format = json.dumps(parsed_data)

    return json_format


# JSON second graph
def entity():

    call_function = number_entity()
    parsed_data = call_function.to_dict(orient='records')

    json_data = json.dumps(parsed_data)  
 
    return json_data


# JSON DE sert mais je ne sais pas a quoi
def entity2():

    call_function = number_entity2()
    parsed_data = call_function.to_dict(orient='records')

    json_data = json.dumps(parsed_data) 
 
    return json_data



###########################################################################   JSON SUMMARY FUNCTION    ###########################################################################



def json_number_false(entity, date1, date2):
   
   return ( {
    "counts": str(number_label_false(entity, date1, date2))
    })


def json_number_true(entity, date1, date2):

    return ( {
    "counts": str(number_label_true(entity, date1, date2))
    })

def json_number_mixture(entity, date1, date2):

    return ( {
    "counts": str(number_label_mixture(entity, date1, date2))
    })

def json_number_other(entity, date1, date2):

   return ( {
    "counts": str(number_label_other(entity, date1, date2))
    })




########################################################################    JSON FUNCTION WITH PARAMETER     ###################################################################

 

import ast
# Converts a string representation of a set to a list of its elements.
def string_to_set(string):
    set_str = ast.literal_eval(string)

    if isinstance(set_str, set):
        return list(set_str)

    return []

#JSON fourth graph dashboard with filtered 
def list_resume_claims_per_langues(entity, date1 ,date2):

    function_call = langue_per_label(entity, date1, date2)
    parsed_data = function_call.to_dict(orient='records')

    for item in parsed_data:
        language_code = item['reviewBodyLang']  
        language_name = changecode_langue(language_code)
        item['reviewBodyLang'] = language_name

    json_data = json.dumps(parsed_data) 

    return json_data

#JSON second graph dashboard with filtered 
def list_resume_borne_date1_date2_entity(entity, date1, date2):  

    claims_per_dat_label = borne_entity(entity, date1, date2)
    parsed_data = claims_per_dat_label.to_dict(orient='records')

    json_data = json.dumps(parsed_data)  

    return json_data



#JSON fonction filtrage graphe 3 dashboard
def list_resume_born_source_label(entity, dat1, dat2):

    claims_per_langue_label = born_per_source_label(entity, dat1, dat2)
    parsed_data = claims_per_langue_label.to_dict(orient='records')

    json_data = json.dumps(parsed_data)

    return json_data


#JSON filtrage  graphe 1
def list_resume_born_per_date_label(entity, dat1, dat2, granularite):

    claims_per_dat_label = born_per_date_label(entity, dat1, dat2, granularite)
    parsed_data = claims_per_dat_label.to_dict(orient='records')

    json_data= json.dumps(parsed_data)

    return json_data 


#json for sixth function 
def list_resume_born_topics(date1=None, date2=None):
    df_filtered = born_per_topics_date(date1, date2)
    topic_truth_counts = defaultdict(lambda: {'true': 0, 'false': 0, 'mixture': 0, 'other': 0})

    df_filtered['topic'] = df_filtered['topic'].astype(str)

    for _, row in df_filtered.iterrows():
        topics = [topic.strip() for topic in row['topic'].split(',') if topic.strip()]
        truth_value = row['label']
        for topic in topics:
            if truth_value in topic_truth_counts[topic]:
                topic_truth_counts[topic][truth_value] += 1
                
    # Convert to a list of dictionaries for easier JSON response
    top_topics_list = [{'topics': topic, **counts} for topic, counts in topic_truth_counts.items()]

    # Sort topics by total counts and select the top N
    top_topics_list = sorted(top_topics_list, key=lambda x: -(x['true'] + x['false'] + x['mixture'] + x['other']))

    return top_topics_list


################################################################################    SEARCH PART    #############################################################################

def common_categories():
    df_other_cleaned = df_other.dropna(subset=['topic'])
    df_other_cleaned['topic'] = df_other_cleaned['topic'].apply(lambda x: ', '.join([cat.strip() for cat in x.split(',') if cat.strip()]))
    df_filtered = df_other_cleaned[df_other_cleaned['topic'].str.contains(",")]
    topic_counts = Counter(df_filtered['topic'])
    top_topics = topic_counts.most_common(40)

    # Convert to a list of dictionaries for easier JSON response
    top_topics_list = [{'topic': category, 'count': count} for category, count in top_topics]

    return top_topics_list



def create_graph_data():
    data = common_categories()
    nodes_set = set()
    edges = []

    for entry in data:
        topics = entry["topic"].split(", ")
        count = entry["count"]
        for topic in topics:
            nodes_set.add(topic)

        # Create edges for all combinations of topics (for 2 to 5 topics)
        if 2 <= len(topics):
            for combo in combinations(topics, 2):
                edges.append({"from": combo[0], "to": combo[1], "weight": count})

    nodes = [{"id": idx + 1, "label": node} for idx, node in enumerate(nodes_set)]
    nodes_map = {node['label']: node['id'] for node in nodes}
    edges_mapped = [{"from": nodes_map[edge["from"]], "to": nodes_map[edge["to"]], "value": edge["weight"]} for edge in edges]
    return nodes, edges_mapped


def top_categories_separated(nbr_categories=60):

    df_other_cleaned = df_other.dropna(subset=['topic'])

    # Vectorized split and explode
    df_other_cleaned['topics'] = df_other_cleaned['topic'].str.split(',')
    df_exploded = df_other_cleaned.explode('topics')
    df_exploded['topics'] = df_exploded['topics'].str.strip()
    df_exploded = df_exploded[df_exploded['topics'] != '']

    # Group by topic and truth value, then count occurrences
    grouped = df_exploded.groupby(['topics', 'label']).size().unstack(fill_value=0)
    grouped = grouped.rename(columns={col: col.lower() for col in grouped.columns})
    for label in ['true', 'false', 'mixture', 'other']:
        if label not in grouped.columns:
            grouped[label] = 0

    grouped = grouped[['true', 'false', 'mixture', 'other']]

    # Convert to a list of dictionaries
    top_topics_list = grouped.reset_index().to_dict('records')

    # Sort topics by total counts and select the top N
    top_topics_list = sorted(top_topics_list, key=lambda x: -(x['true'] + x['false'] + x['mixture'] + x['other']))[:nbr_categories]

    return top_topics_list

'''
This version returns the weight of every single topic 




def common_categories(nbr_categories=60):
    df_other_cleaned = df_other.dropna(subset=['topic'])

    df_other_cleaned['topic'] = df_other_cleaned['topic'].apply(lambda x: ', '.join([cat.strip() for cat in x.split(',') if cat.strip()]))
    df_other_cleaned = df_other_cleaned[df_other_cleaned['topic'] != '']

    topic_counts = Counter(df_other_cleaned['topic'])
    top_topics = topic_counts.most_common(nbr_categories)
    # Convert to a list of dictionaries for easier JSON response
    top_topics_list = [{'topic': category, 'count': count} for category, count in top_topics]

    return top_topics_list
    
def create_graph_data():
    data = common_categories()
    nodes_dict = {}
    edges_dict = {}

    for entry in data:
        topics = entry["topic"].split(", ")
        count = entry["count"]
        for topic in topics:
            if topic not in nodes_dict:
                nodes_dict[topic] = {"id": len(nodes_dict) + 1, "label": topic, "value": 0}
            nodes_dict[topic]["value"] += count

        if len(topics) >= 2:
            for combo in combinations(topics, 2):
                sorted_combo = tuple(sorted(combo))
                if sorted_combo not in edges_dict:
                    edges_dict[sorted_combo] = 0
                edges_dict[sorted_combo] += count
    nodes = [{"id": node_data["id"], "label": node, "value": node_data["value"]} for node, node_data in nodes_dict.items()]
    nodes_map = {node['label']: node['id'] for node in nodes}
    edges_mapped = [{"from": nodes_map[edge[0]], "to": nodes_map[edge[1]], "value": value} for edge, value in edges_dict.items()]

    return nodes, edges_mapped
'''
### Suggestions for the searching part 

## This will return entities that has at least 3 entities prioritizing the most popular entities and the exact entity searched if it exists
def suggestions(query):
    try:
        # Normalize case and filter entries
        suggestions = df_complete[df_complete['entity'].fillna('').str.contains(query, case=False, na=False)]['entity']
        suggestions_lower = suggestions.str.lower()
        # Count occurrences and filter
        entity_counts = suggestions_lower.value_counts()
        # Filter original DataFrame for entities that appear 3 or more times
        frequent_entities = entity_counts[entity_counts >= 3]
        # Sort entities by count (descending) and length of entity name (ascending)
        frequent_entities_sorted = sorted(frequent_entities.items(), key=lambda x: (-x[1], len(x[0])))
        # Check if the normalized query exists in the frequent entities and prioritize it
        normalized_query = query.lower()
        result = [entity[0] for entity in frequent_entities_sorted]
        # If the query exists in the results, prioritize it
        if normalized_query in result:
            # Move the query to the front of the list
            result.insert(0, result.pop(result.index(normalized_query)))
        return jsonify(result)

    except Exception as e:
        current_app.logger.error(f'Error processing request: {str(e)}')
        return jsonify(error=str(e)), 500
    
def suggestionsEntityTopic(query,topic):
    try:
        # Normalize case and filter entries
        df_other['topic'] = df_other['topic'].astype(str).fillna('')
        topic_filter = df_other['topic'].str.contains(topic, case=False, na=False)
        filtered_df = df_other[topic_filter]
        suggestions = filtered_df[filtered_df['entity'].fillna('').str.contains(query, case=False, na=False)]['entity']
        suggestions_lower = suggestions.str.lower()
        # Count occurrences and filter
        entity_counts = suggestions_lower.value_counts()
        # Filter original DataFrame for entities that appear 3 or more times
        frequent_entities = entity_counts[entity_counts >= 3]
        # Sort entities by count (descending) and length of entity name (ascending)
        frequent_entities_sorted = sorted(frequent_entities.items(), key=lambda x: (-x[1], len(x[0])))
        # Check if the normalized query exists in the frequent entities and prioritize it
        normalized_query = query.lower()
        result = [entity[0] for entity in frequent_entities_sorted]
        # If the query exists in the results, prioritize it
        if normalized_query in result:
            # Move the query to the front of the list
            result.insert(0, result.pop(result.index(normalized_query)))
        return jsonify(result)

    except Exception as e:
        current_app.logger.error(f'Error processing request: {str(e)}')
        return jsonify(error=str(e)), 500

## Retreive themes 
def extract_topics():
    unique_topics = set()
    for prediction in df_other['topic']:
        if isinstance(prediction, str):
            topics = [topic.strip() for topic in prediction.split(',') if topic.strip()]
            unique_topics.update(topics)
    return jsonify(list(unique_topics))

def filter_data_entity(selectedEntities, firstDate=None, lastDate=None):
    # Ensure the 'entity' column is string type and fill NaN with an empty string
    df_complete['entity'] = df_complete['entity'].astype(str).fillna('')

    # Filter by entities case-insensitively
    entity_filter = df_complete['entity'].apply(lambda x: any(entity.lower() in x.lower() for entity in selectedEntities))
    filtered_df = df_complete[entity_filter]
    filtered_df['date1'] = pandas.to_datetime(filtered_df['date1'], errors='coerce')
    # Filter by date range if provided
    if firstDate and lastDate:
        filtered_df = filtered_df[(filtered_df['date1'] >= firstDate) & (filtered_df['date1'] <= lastDate)]

    filtered_df = filtered_df.drop_duplicates(subset='id1')
        # Resample by month
    filtered_df['date1'] = filtered_df['date1'].dt.to_period('M')
    return filtered_df

def filter_data_topic(topic, firstDate=None, lastDate=None):
    df_other['topic'] = df_other['topic'].astype(str).fillna('')
    topic_filter = df_other['topic'].str.contains(topic, case=False, na=False)
    filtered_df = df_other[topic_filter]
    filtered_df['date1'] = pandas.to_datetime(filtered_df['date1'], errors='coerce')
    if firstDate and lastDate:
        filtered_df = filtered_df[(filtered_df['date1'] >= firstDate) & (filtered_df['date1'] <= lastDate)]

    # Resample by month
    filtered_df['date1'] = filtered_df['date1'].dt.to_period('M')

    return filtered_df

def filter_data_topic_entity(selectedEntities, topic, firstDate=None, lastDate=None):
    df_other['entity'] = df_other['entity'].astype(str).fillna('')

    # Filter by entities case-insensitively
    entity_filter = df_other['entity'].apply(lambda x: any(entity.lower() in x.lower() for entity in selectedEntities))
    filtered_df = df_other[entity_filter]

    filtered_df['topic'] = filtered_df['topic'].astype(str).fillna('')
    topic_filter = filtered_df['topic'].str.contains(topic, case=False, na=False)
    filtered_df = filtered_df[topic_filter]

    filtered_df['date1'] = pandas.to_datetime(filtered_df['date1'], errors='coerce')
    if firstDate and lastDate:
        filtered_df = filtered_df[(filtered_df['date1'] >= firstDate) & (filtered_df['date1'] <= lastDate)]

    # Resample by month
    filtered_df['date1'] = filtered_df['date1'].dt.to_period('M')

    return filtered_df

