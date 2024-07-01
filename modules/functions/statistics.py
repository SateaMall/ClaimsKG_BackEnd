import json
import pandas
from modules.dataframes.dataframe_singleton import df_simple
from modules.dataframes.dataframe_singleton import df_keyword
from modules.dataframes.dataframe_singleton import df_entity
from modules.dataframes.dataframe_singleton import df_topic


#################################################   HOME page function   #########################################

def claims_total():
    nb_cw_total = len(df_simple['id2'].unique())
    nb_cr_total = len(df_simple['id1'].unique())
    return nb_cw_total, nb_cr_total

def get_dates():
    min1 = pandas.to_datetime(df_simple['date1'].dropna(), errors='coerce').min()
    max1 = pandas.to_datetime(df_simple['date1'].dropna(), errors='coerce').max()
    min1 = min1.strftime('%B %d, %Y')
    max1 = max1.strftime('%B %d, %Y')
    return min1, max1

def numbers_of_entities():
    nb_entities = len(df_entity['entity'].dropna().unique())
    return nb_entities


######################################################################  SUMMARY FUNCTION  ###########################################################################

def number_label_false(date1, date2):
    df_filtre = df_simple
    df_filtre = df_filtre[(df_filtre['label'] == 'FALSE')]
    if date1 is not None: 
        df_filtre = df_filtre[(df_filtre['date1'] >= date1) & (df_filtre['date1'] <= date2)]
    nb_cw_total = len(df_filtre['id1'].unique())
    return nb_cw_total

def number_label_true(date1, date2):
    df_filtre = df_simple
    df_filtre = df_filtre[(df_filtre['label'] == 'TRUE')]
    if date1 is not None: 
        df_filtre = df_filtre[(df_filtre['date1'] >= date1) & (df_filtre['date1'] <= date2)]
    nb_cw_total = len(df_filtre['id1'].unique())
    return nb_cw_total

def number_label_mixture(date1, date2):
    df_filtre = df_simple
    df_filtre = df_filtre[(df_filtre['label'] == 'MIXTURE')]
    if date1 is not None: 
        df_filtre = df_filtre[(df_filtre['date1'] >= date1) & (df_filtre['date1'] <= date2)]
    nb_cw_total = len(df_filtre['id1'].unique())
    return nb_cw_total

def number_label_other(date1, date2):
    df_filtre = df_simple
    df_filtre = df_filtre[(df_filtre['label'] == 'OTHER')]
    if date1 is not None: 
        df_filtre = df_filtre[(df_filtre['date1'] >= date1) & (df_filtre['date1'] <= date2)]
    nb_cw_total = len(df_filtre['id1'].unique())
    return nb_cw_total


###########################################################################   JSON SUMMARY FUNCTION    ###########################################################################


#### DASHBOARD

def json_number_false(date1, date2):
   
   return ( {
    "counts": str(number_label_false(date1, date2))
    })


def json_number_true(date1, date2):

    return ( {
    "counts": str(number_label_true(date1, date2))
    })

def json_number_mixture(date1, date2):

    return ( {
    "counts": str(number_label_mixture(date1, date2))
    })

def json_number_other(date1, date2):

   return ( {
    "counts": str(number_label_other(date1, date2))
    })


#### HOME-PAGE

def dico_numbers_resume():
    total = claims_total()
    list = [
        {"Numbers of claims ": str(total[0]),
         "Numbers of claims review ": str(total[1]),
         "Since ": str(get_dates()[0]), "to ": str(get_dates()[1]),
         "Numbers of entities ": str(numbers_of_entities()),
        }]

    list_json = json.dumps(list)
    return list_json