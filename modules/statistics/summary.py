import json

from flask import current_app, jsonify
import pandas

from modules.dataframes.dataframe_singleton import df_complete
from modules.dataframes.dataframe_singleton import df_Source_labelFALSE
from modules.dataframes.dataframe_singleton import df_Source_labelMIXTURE
from modules.dataframes.dataframe_singleton import df_Source_labelOTHER
from modules.dataframes.dataframe_singleton import df_Source_labelTRUE

#s'occupe de retourner des données pour les graphes

########################Get number of total claims
def claims_total():
    nb_cw_total = len(df_complete['id2'].unique())
    nb_cr_total = len(df_complete['id1'].unique())
    print(nb_cw_total)
    return nb_cw_total, nb_cr_total

#toutes les claims


########################Get number of total claims
def claims_total_for_df(df_complete):
    nb_cw_total = len(df_complete['id2'].unique())
    # print(nb_cw_total)#28354
    return nb_cw_total

#pareil mais en ne prenant que le deuxieme id


#########################Number of claims with entities
###### todo for the pecentage of entities is query for both review and creative work the claims with entities and make a df for each then calculate individually

def claim_with_entities():
    filtre = df_complete['entity'].notna()
    df_filter = df_complete[filtre]
    nb_cw_with_ent = len(df_filter['id2'].unique())
    nb_cw_with_ent1 = len(df_filter['id1'].unique())
    return nb_cw_with_ent, nb_cw_with_ent1




def percent_claim_with_entities():
    cw = claims_total()[0]
    cr = claims_total()[1]
    nb_cw_w_ent = claim_with_entities()[0]
    nb_cr_w_ent = claim_with_entities()[1]
    percent_ent_cw = round((nb_cw_w_ent / cw * 100), 2)
    percent_ent_cr = round((nb_cr_w_ent / cr * 100), 2)
    return percent_ent_cw, percent_ent_cr


##################################Mean of entities by claims

def avg_ent_per_claims():
    filtre = df_complete['entity'].notna()
    df_filtre = df_complete[filtre]
    filtre_group_notna = df_filtre.groupby(['id1', 'id2'])['entity'].size().reset_index(name='counts')
    moy = round(filtre_group_notna['counts'].mean(), 2)
    all = filtre_group_notna['counts'].sum()
    moy_all = round(all / claims_total()[0], 2)
    return moy, moy_all


#######################################Nous

def claims_per_source_label():
    filtre = df_complete['source'].notna()
    df_filtre = df_complete[filtre]
    filtre2 = df_filtre['label'].notna() 
    df_filtre2 = df_filtre[filtre2]
    filtre_group_notna = df_filtre2.groupby(['id1','source','label'])['source'].size().reset_index(name='counts')

    # Perform another groupby on the result
    final_grouped = filtre_group_notna.groupby(['source', 'label'])['counts'].size().reset_index(name='counts')

    return final_grouped

def claims_per_date_label():
    filtre = df_complete['date1'].str.contains(r'^\d{4}-\d{2}-\d{2}$') & df_complete['date1'].notna()
    df_filtre = df_complete[filtre]
    filtre2 = df_filtre['label'].notna() 
    df_filtre2 = df_filtre[filtre2]
    filtre_group_notna = df_filtre2.groupby(['date1','label'])['date1'].size().reset_index(name='counts')

    return filtre_group_notna

def number_entity():
    filtre = df_complete['entity'].notna()
    df_filtre = df_complete[filtre] 
    filtre_group_notna = df_filtre.groupby(['entity'])['entity'].size().reset_index(name='counts')
    return filtre_group_notna

def claims_per_source_label_true():
    grouped_df = df_Source_labelTRUE.groupby(['source', 'label']).size().reset_index(name='counts')
    print(grouped_df)
    return grouped_df

def claims_per_source_label_false():
    grouped_df = df_Source_labelFALSE.groupby(['source', 'label']).size().reset_index(name='counts')
    print(grouped_df)
    return grouped_df

def claims_per_source_label_mixture():
    grouped_df = df_Source_labelMIXTURE.groupby(['source', 'label']).size().reset_index(name='counts')
    print(grouped_df)
    return grouped_df

def claims_per_source_label_other():
    grouped_df = df_Source_labelOTHER.groupby(['source', 'label']).size().reset_index(name='counts')
    print(grouped_df)
    return grouped_df

def number_label_false():
    counts = df_Source_labelFALSE.groupby(['id1']).size().reset_index(name='counts')['counts']
    print(counts)
    return counts

def number_label_true():
    counts = df_Source_labelTRUE.groupby(['id1']).size().reset_index(name='counts')['counts']
    print(counts)
    return counts

def number_label_mixture():
    counts = df_Source_labelMIXTURE.groupby(['id1']).size().reset_index(name='counts')['counts']
    print(counts)
    return counts

def number_label_other():
    counts = df_Source_labelOTHER.groupby(['id1']).size().reset_index(name='counts')['counts']
    print(counts)
    return counts

def number_entity():
    filtre = df_complete['entity'].notna()
    df_filtre = df_complete.loc[filtre, ['entity']]
    filtre_group_notna = df_filtre['entity'].value_counts().reset_index()
    filtre_group_notna.columns = ['entity', 'counts']
    filtre_group_notna = filtre_group_notna.sort_values('counts', ascending=False).head(10)

    print(filtre_group_notna)
    return filtre_group_notna

############################################################


def moy_ent_per_claims_for_df(dataframe):
    # notna
    filtre = dataframe['entity'].notna()
    df_filtre = dataframe[filtre]
    filtre_group_notna = df_filtre.groupby(['id1', 'id2'])['entity'].size().reset_index(name='counts')
    moy = round(filtre_group_notna['counts'].mean(), 2)
    all = filtre_group_notna['counts'].sum()
    # print(all)
    # ent_unique = df_complete['entity'].unique()
    # print(len(ent_unique))
    moy_all = round(all / claims_total_for_df(dataframe), 2)
    # print(moy_all)
    return moy, moy_all


def claim_with_keywords_for_df(dataframe):
    # ent_unique = df_complete['entity'].unique()
    # print(ent_unique)
    # filtre = df_complete['entity'].notnull()
    filtre_k = dataframe['keywords'].notna()
    # df_filter = df_complete[filter]
    df_filter = dataframe[filtre_k]
    # print(df_filter['entity'])
    nb_cw_with_keywords = len(df_filter['id2'].unique())
    nb_cr_with_keywords = len(df_filter['id1'].unique())
    # print(nb_cw_with_keywords)#18066
    # print(nb_cr_with_keywords)#18089
    return nb_cw_with_keywords, nb_cr_with_keywords


# claim_with_keywords()

def percent_claim_with_keywords(dataframe):
    cw = claims_total_for_df(dataframe)
    nb_cw_w_kw = claim_with_keywords()[0]
    nb_cr_w_kw = claim_with_keywords()[1]
    percent_kw_cw = round((nb_cw_w_kw / cw * 100), 2)
    percent_kw_cr = round((nb_cr_w_kw / cw * 100), 2)
    print(percent_kw_cw)
    print(percent_kw_cr)
    return percent_kw_cw, percent_kw_cr


# percent_claim_with_keywords()

def percent_ent_keywords_for_df(dataframe):
    nb_claims_total = claims_total_for_df(dataframe)
    filtre_2 = dataframe['entity'].notna() & dataframe['keywords'].notna()
    df_filter2 = dataframe[filtre_2]
    # print(df_filter2)
    # nb_c_with_2 = len(df_filter2['id1'].unique())
    nb_c_with_2 = len(df_filter2['id2'].unique())
    print(nb_c_with_2)
    percent_with2 = round((nb_c_with_2 / nb_claims_total * 100), 2)
    print(percent_with2)
    return percent_with2


# percent_ent_keywords()

##############################Mean of keywords per claims

def moy_keywords_per_claims(dataframe):
    nb_claims_total = claims_total_for_df(dataframe)
    # notna
    filtre = dataframe['keywords'].notna()
    df_filtre_k = dataframe[filtre]
    filtre_group_notna = df_filtre_k.groupby(['id2'])['keywords'].size().reset_index(name='counts')
    # filtre_group_notna = df_filtre_k.groupby(['id1','id2'])['keywords'].size().reset_index(name='counts')
    moy_k = round(filtre_group_notna['counts'].mean(), 2)
    # print(moy_k)
    all_k = filtre_group_notna['counts'].sum()
    # print(all)
    # ent_unique = df_complete['entity'].unique()
    # print(len(ent_unique))
    moy_all_k = round(all_k / nb_claims_total, 2)
    # print(moy_all_k)
    return moy_k, moy_all_k


# moy_keywords_per_claims()

#########################Number of claims with keywords
def claim_with_keywords():
    filtre_k = df_complete['keywords'].notna()
    df_filter = df_complete[filtre_k]
    nb_cw_with_keywords = len(df_filter['id2'].unique())
    nb_cr_with_keywords = len(df_filter['id1'].unique())
    return nb_cw_with_keywords, nb_cr_with_keywords


def percent_claim_with_keywords():
    cw = claims_total()[0]
    nb_cw_w_kw = claim_with_keywords()[0]
    nb_cr_w_kw = claim_with_keywords()[1]
    percent_kw_cw = round((nb_cw_w_kw / cw * 100), 2)
    percent_kw_cr = round((nb_cr_w_kw / cw * 100), 2)
    return percent_kw_cw, percent_kw_cr


def percent_ent_keywords():
    nb_claims_total = claims_total()[0]
    filtre_2 = df_complete['entity'].notna() & df_complete['keywords'].notna()
    df_filter2 = df_complete[filtre_2]
    nb_c_with_2 = len(df_filter2['id1'].unique())
    percent_with2 = round((nb_c_with_2 / nb_claims_total * 100), 2)
    return percent_with2


############################Mean of keywords per claims
def avg_keywords_per_claims():
    nb_claims_total = claims_total()[0]
    filtre = df_complete['keywords'].notna()
    df_filtre_k = df_complete[filtre]
    filtre_group_notna = df_filtre_k.groupby(['id1', 'id2'])['keywords'].size().reset_index(name='counts')
    moy_k = round(filtre_group_notna['counts'].mean(), 2)

    all_k = filtre_group_notna['counts'].sum()
    moy_all_k = round(all_k / nb_claims_total, 2)

    return moy_k, moy_all_k


########################Get number of total claims creative work


########################Number of claims with author
def claim_with_author():
    filtre_a = df_complete['author'].notna()
    df_filter = df_complete[filtre_a]
    nb_cw_with_author = len(df_filter['id2'].unique())
    return nb_cw_with_author


# claim_with_author()

########################Percent of claims with author
def percent_claim_with_author():
    nb_claims_total = claims_total()[0]
    nb_cw_w_author = claim_with_author()
    percent_author_cw = round((nb_cw_w_author / nb_claims_total * 100), 2)
    # print(percent_author_cw)
    return percent_author_cw


# percent_claim_with_author()


########################Coverage of claims metadata
def get_3():
    nb_claims_total = claims_total()[0]
    filtre_3 = df_complete['author'].notna() & df_complete['entity'].notna() & df_complete['keywords'].notna()
    df_filter3 = df_complete[filtre_3]
    # print(df_filter3)
    nb_c_with_3 = len(df_filter3['id2'].unique())
    # print(nb_c_with_3)
    percent_with3 = round((nb_c_with_3 / nb_claims_total * 100), 2)
    # print(percent_with3)
    return percent_with3


# get_3()

def get_4():
    nb_claims_total = claims_total()[0]
    filtre_4_cw = df_complete['author'].notna() & df_complete['entity'].notna() & df_complete['keywords'].notna() & \
                  df_complete['date2'].notna()
    df_filter4 = df_complete[filtre_4_cw]
    # print(df_filter4)
    nb_cw_with_4 = len(df_filter4['id2'].unique())
    # print(nb_cw_with_4)
    percent_with4 = round((nb_cw_with_4 / nb_claims_total * 100), 2)
    # print(percent_with4)
    return percent_with4


# get_4()


def total_claim_review():
    nb_cr_total = len(df_complete['id1'].unique())
    # print(nb_cr_total)
    return nb_cr_total


# total_claim_review()

def get_dates():
    min1 = pandas.to_datetime(df_complete['date1'].dropna(), errors='coerce').min()
    max1 = pandas.to_datetime(df_complete['date1'].dropna(), errors='coerce').max()
    min2 = pandas.to_datetime(df_complete['date2'].dropna(), errors='coerce').min()
    max2 = pandas.to_datetime(df_complete['date2'].dropna(), errors='coerce').max()

    # min1 = pd.to_datetime(min1.dt.strftime('%B %d, %Y'))
    min1 = min1.strftime('%B %d, %Y')
    # print(min1)
    max1 = max1.strftime('%B %d, %Y')
    # min2 = min2.strftime('%B %d, %Y')
    # min2 = min2.strftime('%B %d, %Y')
    # max2 = max2.strftime('%B %d, %Y')

    return min1, max1, min2, max2


def claim_with_dates():
    filtre_cw = df_complete['date2'].notna()
    df_filter_cw = df_complete[filtre_cw]
    nb_cw_with_dates = len(df_filter_cw['id2'].unique())

    filtre_cr = df_complete['date1'].notna()
    df_filter_cr = df_complete[filtre_cr]
    nb_cr_with_dates = len(df_filter_cr['id1'].unique())

    return nb_cw_with_dates, nb_cr_with_dates


def percent_claim_with_dates():
    total = claims_total()
    nb_cw_w_dates = claim_with_dates()[0]
    nb_cr_w_dates = claim_with_dates()[1]
    percent_dates_cw = round((nb_cw_w_dates / total[0] * 100), 2)
    percent_dates_cr = round((nb_cr_w_dates / total[1] * 100), 2)

    return percent_dates_cw, percent_dates_cr


def numbers_of_author():
    nb_author = len(df_complete['author'].dropna().unique())

    return nb_author


def numbers_of_entities():
    nb_entities = len(df_complete['entity'].dropna().unique())

    return nb_entities


def numbers_keywords():
    nb_keywords = len(df_complete['keywords'].dropna().unique())

    return nb_keywords


def list_numbers_resume():
    total = claims_total()
    list_numbers_res = []
    claims = "Numbers of claims : " + str(total[0])
    claims_review = "Numbers of claims review : " + str(total_claim_review())
    dates = "Since " + str(get_dates()[2]) + " to " + str(get_dates()[1])
    author = "Numbers of authors : " + str(numbers_of_author())
    entities = "Numbers of entities : " + str(numbers_of_entities())
    keywords = "Numbers of keywords : " + str(numbers_keywords())

    list_numbers_res.append(str(claims))
    list_numbers_res.append(str(claims_review))
    list_numbers_res.append(str(dates))
    list_numbers_res.append(str(author))
    list_numbers_res.append(str(entities))
    list_numbers_res.append(str(keywords))

    list_numbers_resume_JSON = json.dumps(list_numbers_res)

    return list_numbers_res, list_numbers_resume_JSON


def dico_numbers_resume():
    total = claims_total()
    list = [
        {"Numbers of claims ": str(total[0]),
         "Numbers of claims review ": str(total_claim_review()),
         "Since ": str(get_dates()[2]), "to ": str(get_dates()[1]),
         #"Numbers of authors ": str(numbers_of_author()),
         "Numbers of entities ": str(numbers_of_entities()),
         #"Numbers of keywords ": str(numbers_keywords())
         }]

    list_json = json.dumps(list)
    # print(list_json)
    return list_json
    
def json_per_source_label_true():

    grouped_label = claims_per_source_label_true()
    json_grouped = []

    for i in range(len(grouped_label)):
        json_grouped.append( {
        "source": str(grouped_label['source'][i]),
        "counts": str(grouped_label['counts'][i])
        })

    return json.dumps(json_grouped)

def json_per_source_label_false():

    grouped_label = claims_per_source_label_false()
    json_grouped = []

    for i in range(len(grouped_label)):
        json_grouped.append( {
        "source": str(grouped_label['source'][i]),
        "counts": str(grouped_label['counts'][i])
        })

    return json.dumps(json_grouped)

def json_per_source_label_mixture():

    grouped_label = claims_per_source_label_mixture()
    json_grouped = []

    for i in range(len(grouped_label)):
        json_grouped.append( {
        "source": str(grouped_label['source'][i]),
        "counts": str(grouped_label['counts'][i])
        })

    return json.dumps(json_grouped)

def json_per_source_label_other():

    grouped_label = claims_per_source_label_other()
    json_grouped = []

    for i in range(len(grouped_label)):
        json_grouped.append( {
        "source": str(grouped_label['source'][i]),
        "counts": str(grouped_label['counts'][i])
        })

    return json.dumps(json_grouped)

def json_number_false():

    return ( {
    "counts": str(len(number_label_false()))
    })

def json_number_true():

    return ( {
    "counts": str(len(number_label_true()))
    })

def json_number_mixture():

    return ( {
    "counts": str(len(number_label_mixture()))
    })

def json_number_other():

    return ( {
    "counts": str(len(number_label_other()))
    })

def json_per_source_label():
    claims_per_source_label_fetch = claims_per_source_label()
    data_length= len(claims_per_source_label_fetch)
    list = []


    for i in range(data_length):
        list.append( {
        "Source": str(claims_per_source_label_fetch['source'][i]),
        "Label": str(claims_per_source_label_fetch['label'][i]),
        "Numbers of claims": str(claims_per_source_label_fetch['counts'][i])
        })

    list_json = json.dumps(list)
    # print(list_json)
    return list_json

def json_per_date1_label(date1,date2):
    print(date1)
    list_claims = claims_per_date_label()
    data_length= len(list_claims)
    list_claims_gerer = []


    for i in range(data_length):
        list_claims_gerer.append( {
        "Date1": str(list_claims['date1'][i]),
        "Label": str(list_claims['label'][i]),
        "Numbers of claims": str(list_claims['counts'][i])
        })

    list_json = json.dumps(list_claims_gerer)
    # print(list_json)
    return list_json

def json_per_entity():
    number_entity_fetch = number_entity()
    data_length= len(number_entity_fetch)
    list = []

    for i in range(data_length):
        list.append( {
        "Entity": str(number_entity_fetch['entity'][i]),
        "Numbers of claims": str(number_entity_fetch['counts'][i])
        })

    list_json = json.dumps(list)
    # print(list_json)
    return list_json


### Suggestions for the searching part
def suggestions(query):
    try:
        suggestions = df_complete[df_complete['entity'].fillna('').str.contains(query, case=False, na=False)]
 
        matches = suggestions['entity'].drop_duplicates().tolist() 
        return jsonify(matches)
    except Exception as e:
        current_app.logger.error(f'Error processing request: {str(e)}')
        return jsonify(error=str(e)), 500