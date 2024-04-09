import json

import pandas as pd
import plotly
import plotly.graph_objs as go

from modules.dataframes.dataframe_singleton import df_complete


def create_scatter_label_source():
    df_complete_filtered = df_complete[['id1', 'id2', 'source', 'date2', 'label']]

    source_sd_duplicate = ['factscan', 'politifact', 'snopes']
    scatter_label_source_json = []
    for source1 in source_sd_duplicate:
        if (str(source1) != "nan"):
            df_plolitifact = df_complete_filtered[df_complete_filtered['source'] == str(source1)]
            df_plolitifact['date2'] = pd.to_datetime(df_plolitifact['date2'])

            df_plolitifact = df_plolitifact[df_plolitifact['date2'] < pd.datetime.today()]
            df_result = df_plolitifact.groupby(['label', pd.Grouper(key='date2', freq='Y')])['id1'].size().reset_index(
                name='counts')

            trace4 = go.Scatter(
                x=df_result[df_result['label'] == 'TRUE']['date2'],  # dfindex,
                y=df_result[df_result['label'] == 'TRUE']['counts'],  # df,
                name='TRUE',
                mode='lines+markers')
            trace3 = go.Scatter(
                x=df_result[df_result['label'] == 'FALSE']['date2'],  # dfindex,
                y=df_result[df_result['label'] == 'FALSE']['counts'],  # df,
                name='FALSE',
                mode='lines+markers')
            trace2 = go.Scatter(
                x=df_result[df_result['label'] == 'OTHER']['date2'],  # dfindex,
                y=df_result[df_result['label'] == 'OTHER']['counts'],  # df,
                name='OTHER',
                mode='lines+markers')
            trace1 = go.Scatter(
                x=df_result[df_result['label'] == 'MIXTURE']['date2'],  # dfindex,
                y=df_result[df_result['label'] == 'MIXTURE']['counts'],  # df,
                name='MIXTURE',
                mode='lines+markers')
            data = [trace1, trace2, trace4, trace3]
            scatter_label_source_json.append(json.dumps(data, cls=plotly.utils.PlotlyJSONEncoder))

    return scatter_label_source_json


def create_scatter_label_source_mois():
    df_complete_filtered = df_complete[['id1', 'id2', 'source', 'date2', 'label']]
    source_sd_duplicate = list(set(list(df_complete_filtered['source'])))

    source_sd_duplicate = ["snopes", "politifact"]  # ['factscan','politifact','snopes']
    scatter_label_source_jsonmois = []

    df_complete_filtered = df_complete_filtered[df_complete_filtered['date2'].notna()]
    df_complete_filtered['date2'] = pd.to_datetime(df_complete_filtered['date2'])
    df_complete_filtered = df_complete_filtered[df_complete_filtered['date2'] <= pd.datetime.today()]
    maxdate1 = pd.to_datetime(df_complete_filtered[df_complete_filtered['date2'].notna()]['date2'].max())

    import datetime as date
    for source1 in source_sd_duplicate:
        if (str(source1) != "nan"):
            df_plolitifact = df_complete_filtered[df_complete_filtered['source'] == str(source1)]
            df_plolitifact['date2'] = pd.to_datetime(df_plolitifact['date2'])
            df_plolitifact = df_plolitifact[df_plolitifact['date2'] < pd.datetime.today()]
            df_plolitifact[df_plolitifact['date2'].notna()]

            minyear = (maxdate1.year) - 4

            datimedebut = date.datetime(minyear - 1, 12, 1)
            df_plolitifact = df_plolitifact[df_plolitifact['date2'] > datimedebut]

            df_result = df_plolitifact.groupby(['label', pd.Grouper(key='date2', freq='M')])['id1'].size().reset_index(
                name='counts')

            trace4 = go.Scatter(
                x=df_result[df_result['label'] == 'TRUE']['date2'],  # dfindex,
                y=df_result[df_result['label'] == 'TRUE']['counts'],  # df,
                name='TRUE',
                mode='lines+markers')
            trace3 = go.Scatter(
                x=df_result[df_result['label'] == 'FALSE']['date2'],  # dfindex,
                y=df_result[df_result['label'] == 'FALSE']['counts'],  # df,
                name='FALSE',
                mode='lines+markers')
            trace2 = go.Scatter(
                x=df_result[df_result['label'] == 'OTHER']['date2'],  # dfindex,
                y=df_result[df_result['label'] == 'OTHER']['counts'],  # df,
                name='OTHER',
                mode='lines+markers')
            trace1 = go.Scatter(
                x=df_result[df_result['label'] == 'MIXTURE']['date2'],  # dfindex,
                y=df_result[df_result['label'] == 'MIXTURE']['counts'],  # df,
                name='MIXTURE',
                mode='lines+markers')
            data = [trace1, trace2, trace4, trace3]
            scatter_label_source_jsonmois.append(json.dumps(data, cls=plotly.utils.PlotlyJSONEncoder))

    return scatter_label_source_jsonmois
