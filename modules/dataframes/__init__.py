import csv
from copy import deepcopy
import langcodes
import numpy as np
import pandas 
from SPARQLWrapper import SPARQLWrapper, JSON

from sparql.sparql_offset_fetcher import SparQLOffsetFetcher


pandas.set_option('display.max_colwidth', None)
pandas.set_option('display.max_columns', None)
endpoint = "https://data.gesis.org/claimskg/sparql"
sparql = SPARQLWrapper(endpoint)
sparql.setReturnFormat(JSON)

    # https://pandas.pydata.org/pandas-docs/stable/user_guide/missing_data.html
    # Note that pandas/NumPy uses the fact that np.nan != np.nan, and treats None like np.nan.


    
def convert_lang_code_to_name(lang_code):
    try:
        return langcodes.Language.get(lang_code).display_name()
    except:
        return lang_code
    
def get_sparql_dataframe(query: SparQLOffsetFetcher):
    """
    Helper function to convert SPARQL results into a Pandas data frame.
    """
    print("intering get_sparql_dataframe")
    results = query.fetch_all()
    cols = results[0].keys()
    
    out = []
    for row in results:
        item = []
        for c in cols:
            item.append(row.get(c, {}).get('value'))
        out.append(item)
    return pandas.DataFrame(out, columns=cols)

def clean_keyword(keyword): 
    return keyword.strip("[]' ")


def generate_dataframes():
    prefixes = "PREFIX schema: <http://schema.org/> PREFIX nif: <http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#>"
    # prefixe = "PREFIX itsrdf:https://www.w3.org/2005/11/its/rdf PREFIX schema:http://schema.org/ PREFIX dbr:http://dbpedia.org/resource/ "
    quent1 = SparQLOffsetFetcher(sparql, 10000,
                                 prefixes,
                                 """
                ?id1 a schema:ClaimReview.
                ?id1 schema:itemReviewed ?id2.
                ?id1 schema:mentions ?linkent1.
                ?linkent1 nif:isString ?entity.
                                 """, "distinct ?id1 ?id2 ?entity")
    quent2 = SparQLOffsetFetcher(sparql, 10000, prefixes,
                                 """
                ?id1 a schema:ClaimReview.
                ?id1 schema:itemReviewed ?id2.
                ?id2 schema:mentions ?linkent2.
                MINUS {?id1 schema:mentions ?linkent1.
                ?id2 schema:mentions ?linkent1.}
                ?linkent2 nif:isString ?entity.
                                 """, "distinct ?id1 ?id2 ?entity")

    qulabel = SparQLOffsetFetcher(sparql, 10000, prefixes,
                                  """
             ?id1 a schema:ClaimReview.
             ?id1 schema:reviewRating ?nor.
             ?nor schema:author <http://data.gesis.org/claimskg/organization/claimskg>;
             schema:alternateName ?label.
                                  """, "distinct ?id1 ?label")

    qudates_cr = SparQLOffsetFetcher(sparql, 10000, prefixes,
                                     """
                    ?id1 a schema:ClaimReview.
                    ?id1 schema:itemReviewed ?id2.
                    ?id1 schema:datePublished ?date1.
                                     """, "?id1 ?id2 ?date1")
     
    qulang = SparQLOffsetFetcher(sparql, 10000, prefixes,
                                """
                    ?id1 a schema:ClaimReview;
                    schema:reviewBody ?reviewBody.
                    Bind(lang(?reviewBody) AS ?reviewBodyLang)
                                """, "distinct ?id1 ?reviewBodyLang")

    qusources = SparQLOffsetFetcher(sparql, 10000, prefixes,
                                    """
           ?id1 a schema:ClaimReview.
           ?id1 schema:author ?source_temp.
           ?source_temp schema:name ?source.
                                    """, "distinct ?id1 ?source")
    

    qurl = SparQLOffsetFetcher(sparql, 10000, prefixes,
                                    """
           ?id1 a schema:ClaimReview.
           ?id1 schema:url ?claimReview_url.
                                    """, "distinct ?id1 ?claimReview_url")
    

    qwords = SparQLOffsetFetcher(sparql, 10000, prefixes,
                                 """
                    ?id2 a schema:CreativeWork.
                    ?id2 schema:keywords ?keywordsURI.
                    ?keywordsURI schema:name ?keywords.
                                 """, "distinct ?id2 ?keywords")


    df_keywords = get_sparql_dataframe(qwords)
    df_keywords['keywords'] = df_keywords['keywords'].apply(clean_keyword)
    df_entities1 = get_sparql_dataframe(quent1)
    df_entities2 = get_sparql_dataframe(quent2)
    df_langue = get_sparql_dataframe(qulang)
    df_label = get_sparql_dataframe(qulabel)
    df_dates_cr = get_sparql_dataframe(qudates_cr)
    df_sources = get_sparql_dataframe(qusources)
    df_urls = get_sparql_dataframe(qurl)

    # Pretreatements
    df_entities = pandas.concat([df_entities1, df_entities2]).drop_duplicates().reset_index(drop=True)
    df_entities['entity'] = df_entities['entity'].astype(str).fillna('')
    df_langue['reviewBodyLang'] = df_langue['reviewBodyLang'].apply(convert_lang_code_to_name)


    df_date_label=pandas.merge(df_label, df_dates_cr, on=['id1'], how='outer')
    df_date_label_langue = pandas.merge(df_date_label, df_langue, on=['id1'], how='outer')
    df_date_label_langue_sources = pandas.merge(df_date_label_langue, df_sources, on=['id1'], how='outer')
    
        # Pretreament Temporary since we have a source that doesn't have a an @ar "Fatabayano"
    condition = (df_date_label_langue_sources['reviewBodyLang'].isna()) & (df_date_label_langue_sources['source'] == 'fatabyyano')
    df_date_label_langue_sources.loc[condition, 'reviewBodyLang'] = 'Arabic'


    # Dataframe simple
    df_simple = df_date_label_langue_sources

    # Dataframe entities
    # Convert to strings to avoide errors
    df_date_label_langue_sources_entity = pandas.merge(df_entities, df_date_label_langue_sources, on=['id1','id2'], how='outer')
    df_entity = df_date_label_langue_sources_entity




    # Dataframe keywords
    df_date_label_langue_sources_keyword=pandas.merge(df_date_label_langue_sources, df_keywords, on=['id2'], how='outer')
    df_date_label_langue_sources_keyword_url=pandas.merge(df_date_label_langue_sources_keyword,df_urls, on=['id1'], how='outer')
    df_complete_keywords = df_date_label_langue_sources_keyword_url


    


    # Dataframe to csv
    df_entity.to_csv('modules/df_entity.csv', quoting=csv.QUOTE_NONNUMERIC, index=False)
    df_complete_keywords.to_csv('modules/df_keyword.csv', quoting=csv.QUOTE_NONNUMERIC, index=False)
    df_simple.to_csv('modules/df_simple.csv', quoting=csv.QUOTE_NONNUMERIC, index=False)
    
    return 'Global dataframes generation is complete'