import json

import numpy
import pandas
import plotly
import plotly.graph_objs as go

from modules.dataframes.dataframe_singleton import df_complete, df_Source_labelOTHER, df_Source_labelTRUE, df_Source_labelFALSE, \
    df_Source_labelMIXTURE
from modules.statistics.summary import avg_keywords_per_claims, avg_ent_per_claims, percent_claim_with_author, \
    percent_claim_with_entities, percent_claim_with_keywords, percent_ent_keywords, get_3, get_4, \
    moy_keywords_per_claims, percent_claim_with_dates, moy_ent_per_claims_for_df


def create_per_source_piechart():
    # prepare data
    source_notna = df_complete['source'].notna()
    sources_ids = df_complete[source_notna]

    id_unique = sources_ids.drop_duplicates(subset='id1')

    claim_by_sources_id_unique = id_unique.groupby(['source'])['id1'].size().reset_index(name='counts')
    sizes = []
    labels = []

    for i in range(len(claim_by_sources_id_unique['counts'])):
        sizes.append(claim_by_sources_id_unique['counts'][i])
        labels.append(claim_by_sources_id_unique['source'][i])

    # graph to json
    trace = [go.Pie(labels=labels, values=sizes,
                    hoverinfo='label+percent', textinfo='percent',
                    textfont=dict(size=20))]

    piechart_sources_JSON = json.dumps(trace, cls=plotly.utils.PlotlyJSONEncoder)

    return piechart_sources_JSON


def create_truthrating_piechart():
    # prepare data
    labels_notna = df_complete['label'].notna()
    labels_ids = df_complete[labels_notna]

    id_unique = labels_ids.drop_duplicates(subset='id1')

    claim_by_labels_id_unique = id_unique.groupby(['label'])['id1'].size().reset_index(name='counts')

    sizes = []
    labels = []

    for i in range(len(claim_by_labels_id_unique['counts'])):
        sizes.append(claim_by_labels_id_unique['counts'][i])
        labels.append(claim_by_labels_id_unique['label'][i])

    # graph to json
    colors = ['red', 'gold', 'grey', 'green']

    trace = [go.Pie(labels=labels, values=sizes,
                    hoverinfo='label+percent', textinfo='percent',
                    textfont=dict(size=20),
                    marker=dict(colors=colors))]

    piechart_labels_JSON = json.dumps(trace, cls=plotly.utils.PlotlyJSONEncoder)

    return piechart_labels_JSON


def create_source_by_truth_value_barchart():
    df_Source_labelOTHERE_Distinct = list(set(df_Source_labelOTHER["source"]))

    dic = {}
    for auteur in df_Source_labelOTHERE_Distinct:
        A = list(df_Source_labelTRUE['source']).count(auteur)
        B = list(df_Source_labelFALSE['source']).count(auteur)
        C = list(df_Source_labelOTHER['source']).count(auteur)
        D = list(df_Source_labelMIXTURE['source']).count(auteur)
        dic[auteur] = [A, B, C, D]

    labels = ['TRUE', 'FALSE', 'OTHER', 'MIXTURE']

    colors = ['blue', 'lightskyblue', 'red', 'yellow', 'green', 'black', 'orange', 'pink', 'grey', 'teal', 'tomato',
              'gold', 'khaki', 'darkkhaki', 'plum', 'olive']

    data = []
    i = 0
    for key in dic.keys():
        trace0 = go.Bar(
            x=labels,
            y=dic[key],
            name=key,
            marker=dict(color=colors[i]))
        data.append(trace0)
        i = i + 1

    layout = go.Layout(
        title='Means of item by claims',
        barmode='group'
    )

    barchart_nb_means_JSON22 = json.dumps(data, cls=plotly.utils.PlotlyJSONEncoder)

    return barchart_nb_means_JSON22


def create_global_average_barchart():
    average_entities_per_claim = avg_ent_per_claims()
    m_ent_pc_we = average_entities_per_claim[0]
    m_ent_pc = average_entities_per_claim[1]

    average_keywords_per_claim = avg_keywords_per_claims()
    m_kw_pc_wk = average_keywords_per_claim[0]
    m_kw_pc = average_keywords_per_claim[1]

    labels = ['Mean of entities for all claims', 'Mean of entities for claims with entities',
              'Mean of keywords for all claims', 'Mean of keywords for claims with keywords']

    values = [m_ent_pc, m_ent_pc_we, m_kw_pc, m_kw_pc_wk]

    colors = ['blue', 'lightskyblue']

    trace0 = go.Bar(
        x=[labels[0], labels[2]],
        y=[values[0], values[2]],
        name='All claims',
        marker=dict(color=colors[0]))

    trace1 = go.Bar(
        x=[labels[1], labels[3]],
        y=[values[1], values[3]],
        name='Claims with corresponding item',
        marker=dict(color=colors[1]))

    data = [trace0, trace1]

    barchart_means_json = json.dumps(data, cls=plotly.utils.PlotlyJSONEncoder)

    return barchart_means_json


# Get percent coverage of metadata
def create_metadata_coverage_barchart():
    # avec auteur
    percent_with_author = percent_claim_with_author()
    # avec entites
    # percent_with_entities = entites_resume2.percent_claim_with_entities()[1]
    percent_with_entities_cw = percent_claim_with_entities()[0]
    percent_with_entities_cr = percent_claim_with_entities()[1]
    # avec keywords
    percent_with_keywords = percent_claim_with_keywords()[0]
    # avec keywords et entites
    percent_with_ent_keywords = percent_ent_keywords()
    # avec les 3
    percent_with_3 = get_3()
    # with dates published
    # claims creative work
    percent_claims_dates = percent_claim_with_dates()
    percent_dates_cw = percent_claims_dates[0]
    percent_dates_cr = percent_claims_dates[1]
    # with all
    percent_with_4 = get_4()

    labels = ['With author', 'With entities', 'With keywords', 'With dates', 'With keywords and entities',
              'With entities, keywords, author', 'With all']

    values = []
    values.append(percent_with_author)
    values.append(percent_with_entities_cw)
    values.append(percent_with_entities_cr)
    values.append(percent_with_keywords)
    values.append(percent_with_ent_keywords)
    values.append(percent_with_3)
    values.append(percent_dates_cw)
    values.append(percent_dates_cr)
    values.append(percent_with_4)

    colors = ['blue', 'gold']

    trace0 = go.Bar(
        x=labels,
        y=[values[0], values[1], values[3], values[6], values[4], values[5], values[8]],
        name='Percent of claims',
        marker=dict(color=colors[0]))

    trace1 = go.Bar(
        x=[labels[1], labels[3]],
        y=[values[2], values[7]],
        name='Percent of claims reviews',
        marker=dict(color=colors[1]))

    data = [trace0, trace1]

    barchart_percent_global_JSON = json.dumps(data, cls=plotly.utils.PlotlyJSONEncoder)

    return barchart_percent_global_JSON


def create_barchart_nb_means_source():
    # loop on sources
    labels = []
    values = []
    i = 0
    # for source in source1:
    filtre = df_complete['source'] == str('factscan')
    df_filtered = df_complete[filtre]
    # print(df_complete)
    # if str(source)!='nan':
    #     print(str(source))
    m_ent_pc_we = moy_ent_per_claims_for_df(df_filtered)[0]
    # print(m_ent_pc_we)
    m_ent_pc = moy_ent_per_claims_for_df(df_filtered)[1]
    # print(m_ent_pc)
    m_kw_pc_wk = moy_keywords_per_claims(df_filtered)[0]
    # print(m_kw_pc_wk)
    m_kw_pc = moy_keywords_per_claims(df_filtered)[1]
    # print(m_kw_pc)

    if not numpy.isnan(m_ent_pc):
        labels.append('Mean of entities for all claims')
        values.append(m_ent_pc)

    if not numpy.isnan(m_ent_pc_we):
        labels.append('Mean of entities for claims with entities')
        values.append(m_ent_pc_we)

    if not numpy.isnan(m_kw_pc):
        labels.append('Mean of keywords for all claims')
        values.append(m_kw_pc)

    if not numpy.isnan(m_kw_pc_wk):
        labels.append('Mean of keywords for claims with keywords')
        values.append(m_kw_pc_wk)

    # print(values)
    # print(labels)

    colors = ['blue', 'lightskyblue', 'red', 'yellow', 'green', 'black']
    trace0 = go.Bar(
        x=labels,
        y=values,
        name='factscan',
        marker=dict(color=colors[0]))
    ####

    labels1 = []
    values1 = []

    filtre1 = df_complete['source'] == str('truthorfiction')
    df_filtered = df_complete[filtre1]
    # print(df_complete)
    # if str(source)!='nan':
    #     print(str(source))
    m_ent_pc_we = moy_ent_per_claims_for_df(df_filtered)[0]
    # print(m_ent_pc_we)
    m_ent_pc = moy_ent_per_claims_for_df(df_filtered)[1]
    # print(m_ent_pc)
    m_kw_pc_wk = moy_keywords_per_claims(df_filtered)[0]
    # print(m_kw_pc_wk)
    m_kw_pc = moy_keywords_per_claims(df_filtered)[1]
    # print(m_kw_pc)

    if not numpy.isnan(m_ent_pc):
        labels1.append('Mean of entities for all claims')
        values1.append(m_ent_pc)

    if not numpy.isnan(m_ent_pc_we):
        labels1.append('Mean of entities for claims with entities')
        values1.append(m_ent_pc_we)

    if not numpy.isnan(m_kw_pc):
        labels1.append('Mean of keywords for all claims')
        values1.append(m_kw_pc)

    if not numpy.isnan(m_kw_pc_wk):
        labels1.append('Mean of keywords for claims with keywords')
        values1.append(m_kw_pc_wk)

    trace1 = go.Bar(
        x=labels1,
        y=values1,
        name='truthorfiction',
        marker=dict(color=colors[1]))

    ####

    labels2 = []
    values2 = []

    filtre2 = df_complete['source'] == str('checkyourfact')
    df_filtered = df_complete[filtre2]
    # print(df_complete)
    # if str(source)!='nan':
    #     print(str(source))
    m_ent_pc_we = moy_ent_per_claims_for_df(df_filtered)[0]
    # print(m_ent_pc_we)
    m_ent_pc = moy_ent_per_claims_for_df(df_filtered)[1]
    # print(m_ent_pc)
    m_kw_pc_wk = moy_keywords_per_claims(df_filtered)[0]
    # print(m_kw_pc_wk)
    m_kw_pc = moy_keywords_per_claims(df_filtered)[1]
    # print(m_kw_pc)

    if not numpy.isnan(m_ent_pc):
        labels2.append('Mean of entities for all claims')
        values2.append(m_ent_pc)

    if not numpy.isnan(m_ent_pc_we):
        labels2.append('Mean of entities for claims with entities')
        values2.append(m_ent_pc_we)

    if not numpy.isnan(m_kw_pc):
        labels2.append('Mean of keywords for all claims')
        values2.append(m_kw_pc)

    if not numpy.isnan(m_kw_pc_wk):
        labels2.append('Mean of keywords for claims with keywords')
        values2.append(m_kw_pc_wk)

    # print(values)
    # print(labels)

    trace2 = go.Bar(
        x=labels2,
        y=values2,
        name='checkyourfact',
        marker=dict(color=colors[2]))

    ####

    labels3 = []
    values3 = []

    filtre3 = df_complete['source'] == str('africacheck')
    df_filtered = df_complete[filtre3]

    m_ent_pc_we = moy_ent_per_claims_for_df(df_filtered)[0]
    m_ent_pc = moy_ent_per_claims_for_df(df_filtered)[1]
    m_kw_pc_wk = moy_keywords_per_claims(df_filtered)[0]
    m_kw_pc = moy_keywords_per_claims(df_filtered)[1]

    if not numpy.isnan(m_ent_pc):
        labels3.append('Mean of entities for all claims')
        values3.append(m_ent_pc)

    if not numpy.isnan(m_ent_pc_we):
        labels3.append('Mean of entities for claims with entities')
        values3.append(m_ent_pc_we)

    if not numpy.isnan(m_kw_pc):
        labels3.append('Mean of keywords for all claims')
        values3.append(m_kw_pc)

    if not numpy.isnan(m_kw_pc_wk):
        labels3.append('Mean of keywords for claims with keywords')
        values3.append(m_kw_pc_wk)

    # print(values)
    # print(labels)

    # colors = ['blue', 'lightskyblue', 'red', 'yellow', 'green', 'black']
    trace3 = go.Bar(
        x=labels3,
        y=values3,
        name='africacheck',
        marker=dict(color=colors[3]))

    ####

    labels4 = []
    values4 = []

    filtre4 = df_complete['source'] == str('snopes')
    df_filtered = df_complete[filtre4]
    # print(df_complete)
    # if str(source)!='nan':
    #     print(str(source))
    m_ent_pc_we = moy_ent_per_claims_for_df(df_filtered)[0]
    # print(m_ent_pc_we)
    m_ent_pc = moy_ent_per_claims_for_df(df_filtered)[1]
    # print(m_ent_pc)
    m_kw_pc_wk = moy_keywords_per_claims(df_filtered)[0]
    # print(m_kw_pc_wk)
    m_kw_pc = moy_keywords_per_claims(df_filtered)[1]
    # print(m_kw_pc)

    if not numpy.isnan(m_ent_pc):
        labels4.append('Mean of entities for all claims')
        values4.append(m_ent_pc)

    if not numpy.isnan(m_ent_pc_we):
        labels4.append('Mean of entities for claims with entities')
        values4.append(m_ent_pc_we)

    if not numpy.isnan(m_kw_pc):
        labels4.append('Mean of keywords for all claims')
        values4.append(m_kw_pc)

    if not numpy.isnan(m_kw_pc_wk):
        labels4.append('Mean of keywords for claims with keywords')
        values4.append(m_kw_pc_wk)

    # print(values)
    # print(labels)

    trace4 = go.Bar(
        x=labels4,
        y=values4,
        name='snopes',
        marker=dict(color=colors[4]))

    ####

    labels5 = []
    values5 = []

    filtre5 = df_complete['source'] == str('politifact')
    df_filtered = df_complete[filtre5]
    # print(df_complete)
    # if str(source)!='nan':
    #     print(str(source))
    m_ent_pc_we = moy_ent_per_claims_for_df(df_filtered)[0]
    # print(m_ent_pc_we)
    m_ent_pc = moy_ent_per_claims_for_df(df_filtered)[1]
    # print(m_ent_pc)
    m_kw_pc_wk = moy_keywords_per_claims(df_filtered)[0]
    # print(m_kw_pc_wk)
    m_kw_pc = moy_keywords_per_claims(df_filtered)[1]
    # print(m_kw_pc)

    if not numpy.isnan(m_ent_pc):
        labels5.append('Mean of entities for all claims')
        values5.append(m_ent_pc)

    if not numpy.isnan(m_ent_pc_we):
        labels5.append('Mean of entities for claims with entities')
        values5.append(m_ent_pc_we)

    if not numpy.isnan(m_kw_pc):
        labels5.append('Mean of keywords for all claims')
        values5.append(m_kw_pc)

    if not numpy.isnan(m_kw_pc_wk):
        labels5.append('Mean of keywords for claims with keywords')
        values5.append(m_kw_pc_wk)

    # print(values)
    # print(labels)

    trace5 = go.Bar(
        x=labels5,
        y=values5,
        name='politifact',
        marker=dict(color=colors[5]))

    data = [trace0, trace1, trace2, trace3, trace4, trace5]

    layout = go.Layout(
        title='Means of item by claims',
        barmode='group'
    )
    # print(data)

    barchart_nb_means_JSON_Source = json.dumps(data, cls=plotly.utils.PlotlyJSONEncoder)

    # print(piechart_labels_JSON)
    return barchart_nb_means_JSON_Source


def scatterplot_authors_social_media_label():
    labels_notna = df_complete['label'].notna()
    df_complete_filtered = df_complete.copy()
    df_complete_filtered['author'] = df_complete_filtered['author'].str.lower()

    # trace 0
    facebook = 'facebook posts'
    filtre_author1 = df_complete_filtered['author'] == facebook

    df_author1 = df_complete_filtered[filtre_author1 & labels_notna]

    # add groupby dates for scatter
    id_unique1 = df_author1.drop_duplicates(subset='id1')

    id_unique1['date1'] = pandas.to_datetime(id_unique1['date1'])
    df_result1 = id_unique1.groupby(['label', pandas.Grouper(key='date1', freq='Y')])['id1'].size().reset_index(
        name='counts')

    # filter here
    filtre_result1_t = df_result1['label'] == 'TRUE'
    df_result1_t_filter = df_result1[filtre_result1_t]

    filtre_result1_m = df_result1['label'] == 'MIXTURE'
    df_result1_m_filter = df_result1[filtre_result1_m]

    filtre_result1_f = df_result1['label'] == 'FALSE'
    df_result1_f_filter = df_result1[filtre_result1_f]

    # trace to scatter
    trace0 = go.Scatter(
        x=df_result1_t_filter['date1'],
        y=df_result1_t_filter['counts'],
        mode='lines+markers',
        name='Facebook posts : True',
        marker=dict(color='#F400A1'))

    trace1 = go.Scatter(
        x=df_result1_m_filter['date1'],
        y=df_result1_m_filter['counts'],
        mode='lines+markers',
        name='Facebook posts : Mixture',
        marker=dict(color='#C71585'))

    trace2 = go.Scatter(
        x=df_result1_f_filter['date1'],
        y=df_result1_f_filter['counts'],
        mode='lines+markers',
        name='Facebook posts : False',
        marker=dict(color='#811453'))

    ##############################
    # bloggers
    bloggers = 'bloggers'
    filtre_author2 = df_complete_filtered['author'] == bloggers

    df_author2 = df_complete_filtered[filtre_author2 & labels_notna]
    # print(df_author2)

    id_unique2 = df_author2.drop_duplicates(subset='id1')

    id_unique2['date1'] = pandas.to_datetime(id_unique2['date1'])
    df_result2 = id_unique2.groupby(['label', pandas.Grouper(key='date1', freq='Y')])['id1'].size().reset_index(
        name='counts')
    # print(df_result2)

    # filter here
    filtre_result2_t = df_result2['label'] == 'TRUE'
    df_result2_t_filter = df_result2[filtre_result2_t]
    # print(df_result2_t_filter)

    filtre_result2_m = df_result2['label'] == 'MIXTURE'
    df_result2_m_filter = df_result2[filtre_result2_m]
    # print(df_result2_m_filter)

    filtre_result2_f = df_result2['label'] == 'FALSE'
    df_result2_f_filter = df_result2[filtre_result2_f]
    # print(df_result2_f_filter)

    # trace to scatter
    trace3 = go.Scatter(
        x=df_result2_t_filter['date1'],
        y=df_result2_t_filter['counts'],
        mode='lines+markers',
        name='Bloggers : True',
        marker=dict(color='#7B68EE'))

    trace4 = go.Scatter(
        x=df_result2_m_filter['date1'],
        y=df_result2_m_filter['counts'],
        mode='lines+markers',
        name='Bloggers : Mixture',
        marker=dict(color='#6600FF'))

    trace5 = go.Scatter(
        x=df_result2_f_filter['date1'],
        y=df_result2_f_filter['counts'],
        mode='lines+markers',
        name='Bloggers : False',
        marker=dict(color='#2E006C'))

    data = [trace0, trace1, trace2, trace3, trace4, trace5]

    scatter_internet_JSON = json.dumps(data, cls=plotly.utils.PlotlyJSONEncoder)

    return scatter_internet_JSON
