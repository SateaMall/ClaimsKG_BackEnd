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
    print(nb_cw_total)
    return nb_cw_total, nb_cr_total


def total_claim_review():
    nb_cr_total = len(df_complete['id1'].unique())
    # print(nb_cr_total)
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


############################################################    FONCTION GRAPHE SIMPLE  #####################################################################

def claims_per_source_label():
    filtre = df_complete['source'].notna()
    df_filtre = df_complete[filtre]
    filtre2 = df_filtre['label'].notna() 
    df_filtre2 = df_filtre[filtre2]
    filtre_group_notna = df_filtre2.groupby(['id1','source','label'])['source'].size().reset_index(name='counts')

    # Perform another groupby on the result
    final_grouped = filtre_group_notna.groupby(['source', 'label'])['counts'].size().reset_index(name='counts')

    return final_grouped


def number_entity():
    filtre = df_complete['entity'].notna()
    df_filtre = df_complete[filtre] 
    filtre_group_notna = df_filtre.groupby(['entity'])['entity'].size().reset_index(name='counts')
    return filtre_group_notna


def claims_topics():
    filtre = df_other['topics'].notna()
    df_filtre = df_other[filtre]
    df_filtre['topics_count'] = df_filtre['topics'].apply(lambda x: len(eval(x)))
    df_filtre = df_filtre[df_filtre['topics_count'] >= 2].reset_index(drop=True)
    filtre_group_notna = df_filtre.groupby(['topics'])['topics'].size().reset_index(name='counts')

    filtre_group_notna_sorted = filtre_group_notna.sort_values(by='counts', ascending=False)

    return filtre_group_notna_sorted

def number_entity2():
    filtre = df_complete['entity'].notna()
    df_filtre = df_complete[filtre]
    filtre_group_notna = df_filtre['entity'].value_counts().reset_index()
    filtre_group_notna.columns = ['entity', 'counts']
    filtre_group_notna = filtre_group_notna.sort_values('counts', ascending=False).head(50)

    print(filtre_group_notna)
    return filtre_group_notna
##############################################################################################################################################


######################################################################  FONTION SUMMARY ###########################################################################


def number_label_false(dat1, dat2):
    df_filtre = df_Source_labelFALSE
    if dat1 is not None: 
        df_filtre = df_Source_labelFALSE[(df_Source_labelFALSE['date1'] >= dat1) & (df_Source_labelFALSE['date1'] <= dat2)]
    counts = df_filtre.groupby(['id1']).size().reset_index(name='counts')['counts']
    print(counts)
    return counts

def number_label_true(dat1, dat2):
    df_filtre = df_Source_labelTRUE
    if dat1 is not None: 
        df_filtre = df_Source_labelTRUE[(df_Source_labelTRUE['date1'] >= dat1) & (df_Source_labelTRUE['date1'] <= dat2)]
    counts = df_filtre.groupby(['id1']).size().reset_index(name='counts')['counts']
    print(counts)
    return counts

def number_label_mixture(dat1, dat2):
    df_filtre = df_Source_labelMIXTURE
    if dat1 is not None: 
        df_filtre = df_Source_labelMIXTURE[(df_Source_labelMIXTURE['date1'] >= dat1) & (df_Source_labelMIXTURE['date1'] <= dat2)]
    counts = df_filtre.groupby(['id1']).size().reset_index(name='counts')['counts']
    print(counts)
    return counts

def number_label_other(dat1, dat2):
    df_filtre = df_Source_labelOTHER
    if dat1 is not None: 
        df_filtre = df_Source_labelOTHER[(df_Source_labelOTHER['date1'] >= dat1) & (df_Source_labelOTHER['date1'] <= dat2)]
    counts = df_filtre.groupby(['id1']).size().reset_index(name='counts')['counts']
    print(counts)
    return counts

###########################################################################################################################################################



##############################################################  FONCTION AVEC PARAMETRE #############################################################################


def borne_entity(entity, dat1, dat2):
    filtre = df_complete['entity'].notna()
    df_filtre = df_complete[filtre]

    if entity is not None:
        if isinstance(entity, list):
            df_filtre = df_filtre[~df_filtre['entity'].isin(entity)]
        else:
            df_filtre = df_filtre[(df_filtre['entity'] != entity)]
    if dat1 is not None:    
        df_filtre = df_filtre[(df_filtre['date1'] >= dat1) & (df_filtre['date1'] <= dat2)]

    filtre_group_notna = df_filtre.groupby(['entity'])['entity'].size().reset_index(name='counts')
    
    return filtre_group_notna.sort_values('counts', ascending=False).head(50)


def born_source(source):
    filtre = df_complete['source'].notna()
    df_filtre = df_complete[filtre]
    filtre2 = df_filtre['label'].notna() 
    df_filtre2 = df_filtre[filtre2]
    df_filtre3 = df_filtre2[(df_filtre2['source'] == source)]

    filtre_group_notna = df_filtre3.groupby(['source','label'])['source'].size().reset_index(name='counts')
    
    return filtre_group_notna


def born_langue_per_label(dat1,dat2):

    filtre = df_complete['reviewBodyLang'].notna()
    df_filtre = df_complete[filtre] 
    filtre2 = df_filtre['label'].notna()
    df_filtre2 = df_filtre[filtre2] 
    df_filtre3 = df_filtre2[(df_filtre2['date1'] >= dat1) & (df_filtre2['date1'] <= dat2)]
    filtre_group_notna = df_filtre3.groupby(['id1','reviewBodyLang', 'label'])['reviewBodyLang'].size().reset_index(name='counts')
    final_grouped = filtre_group_notna.groupby(['reviewBodyLang', 'label'])['counts'].size().reset_index(name='counts')

    return final_grouped

def born_per_source_label(dat1, dat2):
    filtre = df_complete['source'].notna()
    df_filtre = df_complete[filtre]
    filtre2 = df_filtre['label'].notna() 
    df_filtre2 = df_filtre[filtre2]
    df_filtre3 = df_filtre2[(df_filtre2['date1'] >= dat1) & (df_filtre2['date1'] <= dat2)]
    filtre_group_notna = df_filtre3.groupby(['id1','source','label'])['source'].size().reset_index(name='counts')

    # Perform another groupby on the result
    final_grouped = filtre_group_notna.groupby(['source', 'label'])['counts'].size().reset_index(name='counts')

    return final_grouped

def born_per_topics_date(date1, date2):
    filtre = df_other['topics'].notna()
    df_filtre = df_other[filtre]
    filtre2 = df_filtre['creativeWork_datePublished'].notna()
    df_filtre = df_filtre[filtre2]
    df_filtre['topics'] = df_filtre['topics'].str.replace('"', '')
    df_filtre['topics'] = df_filtre['topics'].str.replace("'", '')
    df_filtre['topics'] = df_filtre['topics'].str.replace('[{}]', '', regex=True)
    df_filtre3 = df_filtre[(df_filtre['creativeWork_datePublished'] >= date1) & (df_filtre['creativeWork_datePublished'] <= date2)]
    filtre_group_notna = df_filtre3.groupby(['topics', 'creativeWork_datePublished'])['topics'].size().reset_index(name='counts')
    filtre_group_notna_sorted = filtre_group_notna.sort_values(by='counts', ascending=False)
    print(filtre_group_notna)

    return filtre_group_notna_sorted

def born_per_date_label(date1, date2, granularite):
    filtre = df_complete['date1'].str.contains(r'^\d{4}-\d{2}-\d{2}$') & df_complete['date1'].notna()
    df_filtre = df_complete[filtre]
    filtre2 = df_filtre['label'].notna() 
    df_filtre3 = df_filtre[filtre2]
    if date1 is not None :
        df_filtre3 = df_filtre3[(df_filtre['date1'] >= date1) & (df_filtre['date1'] <= date2)]
    if(granularite=="annee"):
        df_filtre3['date1'] = df_filtre3['date1'].str[:4]
        print(df_filtre3['date1'].str[:4])
    if(granularite == "mois") : 
        df_filtre3['date1'] = df_filtre3['date1'].str[:7] 
    
    filtre_group_notna = df_filtre3.groupby(['id1','date1','label'])['date1'].size().reset_index(name='counts')
    final_grouped = filtre_group_notna.groupby(['date1', 'label'])['counts'].size().reset_index(name='counts')


    return final_grouped


def langue_per_label(dat1, dat2):

    filtre = df_complete['reviewBodyLang'].notna()
    df_filtre = df_complete[filtre] 
    filtre2 = df_filtre['label'].notna()
    df_filtre2 = df_filtre[filtre2]
    if dat1 is not None: 
        filtre3 = df_filtre2['date1'].str.contains(r'^\d{4}-\d{2}-\d{2}$') & df_filtre2['date1'].notna()
        df_filtre2 = df_filtre2[filtre3]
        df_filtre2 = df_filtre2[(df_filtre['date1'] >= dat1) & (df_filtre2['date1'] <= dat2)]
    #filtre_group_notna = df_filtre2.groupby(['id1'])['id1']
    filtre_group_notna = df_filtre2.groupby(['id1','reviewBodyLang', 'label'])['reviewBodyLang'].size().reset_index(name='counts')
    final_grouped = filtre_group_notna.groupby(['reviewBodyLang', 'label'])['counts'].size().reset_index(name='counts')

    return final_grouped


######################################################################################################################################################


#################################################################    FONCTION CHANGE CODE EN LANGUE    #######################################################

def changecode_langue(code):
    try:
        lang = Language.get(code)
        return lang.display_name()
    except ValueError:
        return code



#################################################################   Fontion SATEA   #########################################################################

def entite_per_label_filtre_per_date(entity, label, dat1, dat2):
    filtre = df_complete['label'].notna()
    df_filtre = df_complete[filtre]
    filtre2 = df_filtre['entity'].notna()
    df_filtre2 = df_filtre[filtre2]
    filtre3 = df_filtre2['date1'].str.contains(r'^\d{4}-\d{2}-\d{2}$') & df_filtre2['date1'].notna()
    df_filtre3 = df_filtre2[filtre3]
    # temporaire essayer de comprendre pourquoi nous ne pouvons pas ecrire en majuscule dans l'url
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


##########################################################################  JSON SATEA  ############################################################################### 

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


######################################################################JSON Home Page#####################################
def dico_numbers_resume():
    total = claims_total()
    list = [
        {"Numbers of claims ": str(total[0]),
         "Numbers of claims review ": str(total_claim_review()),
         "Since ": str(get_dates()[0]), "to ": str(get_dates()[1]),
         "Numbers of entities ": str(numbers_of_entities()),
         }]

    list_json = json.dumps(list)
    # print(list_json)
    return list_json


########################################################################## JSON FONCTION SIMPLE ############################################################################

def list_resume_claims_per_source_label():

    call_function = claims_per_source_label()
    parsed_data = call_function.to_dict(orient='records')

    json_format = json.dumps(parsed_data)

    return json_format



def entity():

    claims_per_dat_label = number_entity()
    parsed_data = claims_per_dat_label.to_dict(orient='records')

    json_data = json.dumps(parsed_data)  
 
    return json_data

def entity2():

    claims_per_dat_label = number_entity2()
    parsed_data = claims_per_dat_label.to_dict(orient='records')

    json_data = json.dumps(parsed_data) 
 
    return json_data


#######################################################################################################################################################################







###########################################################################   JSON SUMMARY    ###########################################################################



def json_number_false(date1, date2):
    taille = len(number_label_false(date1, date2))
    return ( {
    "counts": str(taille)
    })

def json_number_true(date1, date2):

    return ( {
    "counts": str(len(number_label_true(date1, date2)))
    })

def json_number_mixture(date1, date2):

    return ( {
    "counts": str(len(number_label_mixture(date1, date2)))
    })

def json_number_other(date1, date2):

    return ( {
    "counts": str(len(number_label_other(date1, date2)))
    })


###############################################################################################################################################################
 








########################################################################    JSON FONCTION PARAMETRE     ###################################################################






def list_resume_borne_source(source):

    claims_per_dat_label = born_source(source)
    parsed_data = claims_per_dat_label.to_dict(orient='records')

    json_data = json.dumps(parsed_data)  

    return json_data


def list_resume_claims_per_topics():

    claims_per_dat_label = claims_topics()
    parsed_data = claims_per_dat_label.to_dict(orient='records')


    json_data = json.dumps(parsed_data) 

    return parsed_data
 

import ast

def string_to_set(string):
    # Utiliser ast.literal_eval() pour évaluer la chaîne en tant qu'expression Python
    set_str = ast.literal_eval(string)

    # Si l'objet évalué est un ensemble, retourner le tableau d'éléments
    if isinstance(set_str, set):
        return list(set_str)

    # Sinon, retourner une liste vide
    return []

print(string_to_set("{'a','b'}"))

def list_resume_claims_per_topics2():
    number_entity_fetch = claims_topics()
    data_length= len(number_entity_fetch)
    list = []

    for i in range(data_length):
        list.append( {
        "topics": string_to_set(number_entity_fetch['topics'][i]),
        "counts": str(number_entity_fetch['counts'][i])
        })

    list_json = json.dumps(list)
    # print(list_json)
    return list_json


def list_resume_claims_per_langues(date1 ,date2):

    claims_per_langue_label = langue_per_label(date1, date2)
    parsed_data = claims_per_langue_label.to_dict(orient='records')

    for item in parsed_data:
        language_code = item['reviewBodyLang']  
        language_name = changecode_langue(language_code)
        item['reviewBodyLang'] = language_name

    json_data = json.dumps(parsed_data) 

    return json_data

def list_resume_borne_date1_date2_entity(entity, date1, date2):  #faire comme pour la fonction "list_resume_borne_source" pour recupérer par ici ce qu'on veut (j'ai mis en brut pour tester)

    claims_per_dat_label = borne_entity(entity, date1, date2)
    parsed_data = claims_per_dat_label.to_dict(orient='records')

    json_data = json.dumps(parsed_data)  

    return json_data


def list_resume_born_claims_per_langues(dat1,dat2):

    claims_per_langue_label = born_langue_per_label(dat1,dat2)
    parsed_data = claims_per_langue_label.to_dict(orient='records')

    for item in parsed_data:
        language_code = item['reviewBodyLang']  
        language_name = changecode_langue(language_code)
        item['reviewBodyLang'] = language_name

    json_data = json.dumps(parsed_data)

    return json_data


def list_resume_born_source_label(dat1,dat2):

    claims_per_langue_label = born_per_source_label(dat1,dat2)
    parsed_data = claims_per_langue_label.to_dict(orient='records')

    json_data = json.dumps(parsed_data)

    return json_data


def list_resume_born_topics(dat1, dat2):
    claims_per_langue_label = born_per_topics_date(dat1,dat2)
    parsed_data = claims_per_langue_label.to_dict(orient='records')

    json_data = json.dumps(parsed_data)

    return json_data


def list_resume_born_per_date_label(dat1, dat2, granularite):

    claims_per_dat_label = born_per_date_label(dat1, dat2, granularite)
    parsed_data = claims_per_dat_label.to_dict(orient='records')

    json_data= json.dumps(parsed_data)

    return json_data 


#########################################################################################################################################################################
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
        print(result)  # Debugging
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
        print(result)  # Debugging
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

