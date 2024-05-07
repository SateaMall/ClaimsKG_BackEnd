import json

import pandas

from modules.dataframes.dataframe_singleton import df_complete
from modules.dataframes.dataframe_singleton import df_other



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
    filtre_group_notna = df_filtre2.groupby(['source','label'])['source'].size().reset_index(name='counts')
    
    return filtre_group_notna

def claims_per_date_label():
    df_sample = df_complete.sample(n=100, random_state=42) 
    filtre = df_sample['date1'].str.contains(r'^\d{4}-\d{2}-\d{2}$') & df_sample['date1'].notna()
    df_filtre = df_sample[filtre]
    filtre2 = df_filtre['label'].notna() 
    df_filtre2 = df_filtre[filtre2]
    filtre_group_notna = df_filtre2.groupby(['date1','label'])['date1'].size().reset_index(name='counts')

    return filtre_group_notna

def number_entity():
    filtre = df_complete['entity'].notna()
    df_filtre = df_complete[filtre] 
    filtre_group_notna = df_filtre.groupby(['entity'])['entity'].size().reset_index(name='counts')
    
    return filtre_group_notna


def newdata():
    filtre = df_other['topics'].notna()
    df_filtre = df_other[filtre]
    df_filtre['topics_count'] = df_filtre['topics'].apply(lambda x: len(eval(x)))
    df_filtre = df_filtre[df_filtre['topics_count'] >= 2].reset_index(drop=True)
    filtre_group_notna = df_filtre.groupby(['topics'])['topics'].size().reset_index(name='counts')

    filtre_group_notna_sorted = filtre_group_notna.sort_values(by='counts', ascending=False)

    return filtre_group_notna_sorted


def langue_per_label():

    filtre = df_complete['headlineLang'].notna()
    df_filtre = df_complete[filtre] 
    filtre2 = df_filtre['label'].notna()
    df_filtre2 = df_filtre[filtre2] 
    filtre_group_notna = df_filtre2.groupby(['headlineLang', 'label'])['headlineLang'].size().reset_index(name='counts')
    
    return filtre_group_notna


def borne_date1_date2(dat1, dat2):
    filtre = df_complete['date1'].str.contains(r'^\d{4}-\d{2}-\d{2}$') & df_complete['date1'].notna()
    df_filtre = df_complete[filtre]
    df_filtre2 = df_filtre[(df_filtre['date1'] >= dat1) & (df_filtre['date1'] <= dat2)]
    filtre2 = df_filtre2['label'].notna() 
    df_filtre3 = df_filtre2[filtre2]
    filtre_group_notna = df_filtre3.groupby(['date1','label'])['label'].size().reset_index(name='counts')

    return filtre_group_notna


def borne_entity(entitie):
    filtre = df_complete['entity'].notna()
    df_filtre = df_complete[filtre]
    df_filtre2 = df_filtre[(df_filtre['entity'] == entitie)]
    filtre_group_notna = df_filtre2.groupby(['entity'])['entity'].size().reset_index(name='counts')
    
    return filtre_group_notna

def borne_entity_per_date(entitie,dat1, dat2):
    
    filtre = df_complete['date1'].str.contains(r'^\d{4}-\d{2}-\d{2}$') & df_complete['date1'].notna()
    df_filtre = df_complete[filtre]
    df_filtre2 = df_filtre[(df_filtre['date1'] >= dat1) & (df_filtre['date1'] <= dat2)]
    filtre2 = df_filtre2['label'].notna() 
    df_filtre3 = df_filtre2[filtre2]
    filtre3 = df_filtre3['entity'].notna()
    df_filtre4 = df_filtre3[filtre3]
    df_filtre5 = df_filtre4[(df_filtre['entity'] == entitie)]
    filtre_group_notna = df_filtre3.groupby(['date1', 'date2', 'entity'])['entity'].size().reset_index(name='counts')
    
    return filtre_group_notna

def born_source(source):
    filtre = df_complete['source'].notna()
    df_filtre = df_complete[filtre]
    filtre2 = df_filtre['label'].notna() 
    df_filtre2 = df_filtre[filtre2]
    df_filtre3 = df_filtre2[(df_filtre2['source'] == source)]

    filtre_group_notna = df_filtre3.groupby(['source','label'])['source'].size().reset_index(name='counts')
    
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
    min2 = min2.strftime('%B %d, %Y')
    # min2 = min2.strftime('%B %d, %Y')
    max2 = max2.strftime('%B %d, %Y')

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


def list_resume_claims_per_source_label():

    claims_per_srcs_label = claims_per_source_label()
    parsed_data = claims_per_srcs_label.to_dict(orient='records')

    l = json.dumps(parsed_data)

    return l


def list_resume_claims_per_date_label():

    claims_per_dat_label = claims_per_date_label()
    parsed_data = claims_per_dat_label.to_dict(orient='records')

    json_data = json.dumps(parsed_data)  # Convertir en une chaîne JSON

    return json_data



def entity():

    claims_per_dat_label = number_entity()
    parsed_data = claims_per_dat_label.to_dict(orient='records')

    json_data = json.dumps(parsed_data)  # Convertir en une chaîne JSON
 
    return json_data




def list_resume_claims_per_topics():

    claims_per_dat_label = newdata()
    parsed_data = claims_per_dat_label.to_dict(orient='records')

    json_data = json.dumps(parsed_data)  # Convertir en une chaîne JSON

    return json_data


def list_resume_claims_per_langues():

    claims_per_langue_label = langue_per_label()
    parsed_data = claims_per_langue_label.to_dict(orient='records')

    json_data = json.dumps(parsed_data)  # Convertir en une chaîne JSON

    return json_data


def list_resume_borne_date1_date2():  #faire comme pour la fonction "list_resume_borne_source" pour recupérer par ici ce qu'on veut (j'ai mis en brut pour tester)

    claims_per_dat_label = borne_date1_date2("2005-03-22", "2020-03-30")
    parsed_data = claims_per_dat_label.to_dict(orient='records')

    json_data = json.dumps(parsed_data)  # Convertir en une chaîne JSON

    return json_data


def list_resume_borne_date1_date2_entity():  #faire comme pour la fonction "list_resume_borne_source" pour recupérer par ici ce qu'on veut (j'ai mis en brut pour tester)

    claims_per_dat_label = borne_date1_date2("2005-03-22", "2020-03-30","")
    parsed_data = claims_per_dat_label.to_dict(orient='records')

    json_data = json.dumps(parsed_data)  # Convertir en une chaîne JSON

    return json_data


def list_resume_borne_entities(): #faire comme pour la fonction "list_resume_borne_source" pour recupérer par ici ce qu'on veut (j'ai mis en brut pour tester)
    param_list= []
    param_list.append("#BlackLivesMatter")
    param_list.append("#BringBackOurGirls")
    param_list.append("#EndSARS")
    claims_per_dat_label = borne_entity(param_list)
    parsed_data = claims_per_dat_label.to_dict(orient='records')

    json_data = json.dumps(parsed_data)  # Convertir en une chaîne JSON

    return json_data


def list_resume_borne_source(source):

    claims_per_dat_label = born_source(source)
    parsed_data = claims_per_dat_label.to_dict(orient='records')

    json_data = json.dumps(parsed_data)  # Convertir en une chaîne JSON

    return json_data



def dico_numbers_resume():
    total = claims_total()
    list = [
        {"Numbers of claims ": str(total[0]),
         "Numbers of claims review ": str(total_claim_review()),
         "Since ": str(get_dates()[2]), "to ": str(get_dates()[1]),
         "Numbers of authors ": str(numbers_of_author()),
         "Numbers of entities ": str(numbers_of_entities()),
         "Numbers of keywords ": str(numbers_keywords())}]

    list_json = json.dumps(list)
    # print(list_json)
    return list_json


def dico_new_data():
    total = newdata()
    list = [
        {"Numbers of claims ": str(total)}]

    list_json = json.dumps(list)
    # print(list_json)
    return list_json

def json_per_source_label():
    data_length= len(claims_per_source_label())
    list = []


    for i in range(data_length):
        list.append( {
        "Source": str(claims_per_source_label()['source'][i]),
        "Label": str(claims_per_source_label()['label'][i]),
        "Numbers of claims": str(claims_per_source_label()['counts'][i])
        })

    list_json = json.dumps(list)
    # print(list_json)
    return list_json

def json_per_date1_label():
    data_length= len(claims_per_date_label())
    list = []


    for i in range(data_length):
        list.append( {
        "Date1": str(claims_per_date_label()['date1'][i]),
        "Label": str(claims_per_date_label()['label'][i]),
        "Numbers of claims": str(claims_per_date_label()['counts'][i])
        })

    list_json = json.dumps(list)
    # print(list_json)
    return list_json

def json_per_entity():
    data_length= len(number_entity())
    list = []

    for i in range(data_length):
        list.append( {
        "Entity": str(number_entity()['entity'][i]),
        "Numbers of claims": str(number_entity()['counts'][i])
        })

    list_json = json.dumps(list)
    # print(list_json)
    return list_json

