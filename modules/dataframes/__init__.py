import csv
from copy import deepcopy
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


def generate_per_label_dataframe():
    prefixes = "PREFIX schema: <http://schema.org/> PREFIX nif: <http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#>"
    label_true = SparQLOffsetFetcher(sparql, 10000,
                                     prefixes,
                                     """
                    ?id1 a schema:ClaimReview.
                    ?id1 schema:reviewRating ?nor.
                    ?nor schema:author <http://data.gesis.org/claimskg/organization/claimskg>;
                    schema:alternateName ?label FILTER regex(str(?label),"TRUE", "i").
                    ?id1 schema:author ?source_temp.
                    ?source_temp schema:name ?source.
                    ?id1 schema:datePublished ?date1.
                    ?id1 schema:itemReviewed ?id2.
                    ?id1 schema:mentions ?linkent1.
                    ?linkent1 nif:isString ?entity.
                                     """, "distinct ?id1 ?source ?label ?date1 ?entity")


    label_false = SparQLOffsetFetcher(sparql, 10000,
                                      prefixes,
                                      """
                     ?id1 a schema:ClaimReview.
                     ?id1 schema:reviewRating ?nor.
                     ?nor schema:author <http://data.gesis.org/claimskg/organization/claimskg>;
                     schema:alternateName ?label FILTER regex(str(?label),"FALSE", "i").
                     ?id1 schema:author ?source_temp.
                     ?source_temp schema:name ?source.
                     ?id1 schema:datePublished ?date1.

                                      """, "distinct ?id1 ?source ?label ?date1")

    label_mixture = SparQLOffsetFetcher(sparql, 10000,
                                        prefixes,
                                        """
                       ?id1 a schema:ClaimReview.
                       ?id1 schema:reviewRating ?nor.
                       ?nor schema:author <http://data.gesis.org/claimskg/organization/claimskg>;
                       schema:alternateName ?label FILTER regex(str(?label),"MIXTURE", "i").
                       ?id1 schema:author ?source_temp.
                       ?source_temp schema:name ?source.
                       ?id1 schema:datePublished ?date1.
                                        """, "distinct ?id1 ?source ?label ?date1")

    label_other = SparQLOffsetFetcher(sparql, 10000,
                                      prefixes,
                                      """
                     ?id1 a schema:ClaimReview.
                     ?id1 schema:reviewRating ?nor.
                     ?nor schema:author <http://data.gesis.org/claimskg/organization/claimskg>;
                     schema:alternateName ?label FILTER regex(str(?label),"OTHER", "i").
                     ?id1 schema:author ?source_temp.
                     ?source_temp schema:name ?source.
                     ?id1 schema:datePublished ?date1.
                                      """, "distinct ?id1 ?source ?label ?date1")

    df_source_label_true = get_sparql_dataframe(label_true)

    df_source_label_false = get_sparql_dataframe(label_false)
    df_source_label_mixture = get_sparql_dataframe(label_mixture)
    df_source_label_other = get_sparql_dataframe(label_other)



    df_source_label_true.to_csv("modules/df_Source_labelTRUE.csv", quoting=csv.QUOTE_NONNUMERIC, na_rep='NaN', index=False)
    df_source_label_false.to_csv("modules/df_Source_labelFALSE.csv", quoting=csv.QUOTE_NONNUMERIC, na_rep='NaN',
                                 index=False)
    df_source_label_mixture.to_csv("modules/df_Source_labelMIXTURE.csv", quoting=csv.QUOTE_NONNUMERIC, na_rep='NaN',
                                   index=False)
    df_source_label_other.to_csv("modules/df_Source_labelOTHER.csv", quoting=csv.QUOTE_NONNUMERIC, na_rep='NaN',
                                 index=False)
    return 'ok dataframe per label generation'


def generate_global_dataframe():
    prefixes = "PREFIX schema: <http://schema.org/> PREFIX nif: <http://persistence.uni-leipzig.org/nlp2rdf/ontologies/nif-core#>"
    prefixe = "PREFIX itsrdf:https://www.w3.org/2005/11/its/rdf PREFIX schema:http://schema.org/ PREFIX dbr:http://dbpedia.org/resource/ "
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
                    schema:author ?author;
                    schema:reviewBody ?reviewBody.
                    Bind(lang(?reviewBody) AS ?reviewBodyLang)
                                """, "distinct ?id1 ?reviewBodyLang")

    qusources = SparQLOffsetFetcher(sparql, 10000, prefixes,
                                    """
           ?id1 a schema:ClaimReview.
           ?id1 schema:author ?source_temp.
           ?source_temp schema:name ?source.
                                    """, "distinct ?id1 ?source")
    
    df_entities = get_sparql_dataframe(quent1)
    df_entities2 = get_sparql_dataframe(quent2)
    df_langue = get_sparql_dataframe(qulang)
    df_label = get_sparql_dataframe(qulabel)
    df_dates_cr = get_sparql_dataframe(qudates_cr)
    df_sources = get_sparql_dataframe(qusources)
    print (df_entities)
    df_entities_complete = pandas.concat([df_entities, df_entities2]).drop_duplicates().reset_index(drop=True)

    df_entites_label = pandas.merge(df_entities_complete, df_label, on=['id1'], how='outer')
    df_entites_label_langue = pandas.merge(df_entites_label, df_langue, on=['id1'], how='outer')
    df_entites_label_datecr = pandas.merge(df_entites_label_langue, df_dates_cr, on=['id1','id2'], how='outer')
    df_entites_label_datescr_sources = pandas.merge(df_entites_label_datecr, df_sources, on=['id1'], how='outer')


    df_complete = df_entites_label_datescr_sources


    # dataframe to csv
    df_complete.to_csv('modules/df_complete.csv', quoting=csv.QUOTE_NONNUMERIC, index=False)
    return 'ok global df complete generation'