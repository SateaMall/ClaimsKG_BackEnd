from flask import current_app, jsonify
import pandas
from modules.dataframes.dataframe_singleton import df_simple
from modules.dataframes.dataframe_singleton import df_keyword
from modules.dataframes.dataframe_singleton import df_entity
from modules.dataframes.dataframe_singleton import df_topic

################################################################################    SEARCH PART    #############################################################################


### Suggestions for the searching part 

## This will return entities that has at least 3 entities prioritizing the most popular entities and the exact entity searched if it exists
def suggestions(query):
    try:
        # Normalize case and filter entries
        suggestions = df_entity[df_entity['entity'].fillna('').str.contains(query, case=False, na=False)]['entity']
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
        df_topic['topic'] = df_topic['topic'].astype(str).fillna('')
        topic_filter = df_topic['topic'].str.contains(topic, case=False, na=False)
        filtered_df = df_topic[topic_filter]
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
    for prediction in df_topic['topic']:
        if isinstance(prediction, str):
            topics = [topic.strip() for topic in prediction.split(',') if topic.strip()]
            unique_topics.update(topics)
    return jsonify(list(unique_topics))

def filter_data_entity(data_frame: pandas.DataFrame, selectedEntities, firstDate=None, lastDate=None):
    data_frame['entity'] = data_frame['entity'].astype(str)
    entity_filter = data_frame['entity'].apply(lambda x: any(entity.lower() in x.lower() for entity in selectedEntities))
    filtered_df = data_frame[entity_filter]
    filtered_df['date1'] = pandas.to_datetime(filtered_df['date1'], errors='coerce')
    # Filter by date range if provided
    if firstDate and lastDate:
        filtered_df = filtered_df[(filtered_df['date1'] >= firstDate) & (filtered_df['date1'] <= lastDate)]

    filtered_df = filtered_df.drop_duplicates(subset='id1')
        # Resample by month
    filtered_df['date1'] = filtered_df['date1'].dt.to_period('M')
    return filtered_df

def filter_data_topic(topic, firstDate=None, lastDate=None):
    df_topic['topic'] = df_topic['topic'].astype(str).fillna('')
    topic_filter = df_topic['topic'].str.contains(topic, case=False, na=False)
    filtered_df = df_topic[topic_filter]
    filtered_df['date1'] = pandas.to_datetime(filtered_df['date1'], errors='coerce')
    if firstDate and lastDate:
        filtered_df = filtered_df[(filtered_df['date1'] >= firstDate) & (filtered_df['date1'] <= lastDate)]

    # Resample by month
    filtered_df['date1'] = filtered_df['date1'].dt.to_period('M')

    return filtered_df

def search_date_graph(df_filtered_entity, selectedEntities=None, topic=None):
    # Exclude specific keywords
    excluded_keywords = [
        'fact check', 'false news', 'fact-check', 'fact checks', 'fake news', 'facebook fact-checks', 'punditfact', 'the news', 'facebook posts', 'online', 'viral content', 'tweet','tweets','facebook','facebooks'
    ]
    # Create additional exclusions for each individual word in selected entities and topics 
    if selectedEntities:
        [excluded_keywords.extend(entity.split())for entity in selectedEntities]
        excluded_keywords.extend(selectedEntities)
    if topic :
        excluded_keywords+= topic.split()
        excluded_keywords.append(topic.lower())
    all_exclusions = [keyword.lower() for keyword in excluded_keywords]
    
    # Filter df_keywords based on df_filtered_entity.id2
    df_keywords_filtered = df_keyword[df_keyword['id2'].isin(df_filtered_entity['id2'])]

    # Convert the 'keywords' column to lowercase for comparison
    df_keywords_filtered['keywords_lower'] = df_keywords_filtered['keywords'].str.lower()

        
    df_keywords_filtered = df_keywords_filtered.drop_duplicates(subset=['id1'])
    # Filter out the rows where 'keywords_lower' is in all_exclusions
    df_filtered = df_keywords_filtered[~df_keywords_filtered['keywords_lower'].isin(all_exclusions)]

    # Drop the temporary 'keywords_lower' column
    df_filtered.drop(columns=['keywords_lower'], inplace=True)

    # Convert 'date1' to datetime, coercing errors to NaT
    df_filtered['date1'] = pandas.to_datetime(df_filtered['date1'], errors='coerce')

    # Drop rows where 'date1' is NaT
    df_filtered = df_filtered.dropna(subset=['date1'])

    # Convert date to month and year format
    df_filtered['month_year'] = df_filtered['date1'].dt.to_period('M')
    unique_claims = df_keywords_filtered.drop_duplicates(subset=['id1'])
    unique_claims['date1'] = pandas.to_datetime(unique_claims['date1'], errors='coerce')
    unique_claims = unique_claims.dropna(subset=['date1'])
    unique_claims['month_year'] = unique_claims['date1'].dt.to_period('M')

    # Aggregate counts of unique claims by month-year and label
    total_counts = unique_claims.groupby(['month_year', 'label']).size().reset_index(name='counts')

    # Find the most recurrent keyword for each month-year-label combination
    keyword_counts = df_filtered.groupby(['month_year', 'label', 'keywords']).size().reset_index(name='counts')
    most_recurrent_keyword = keyword_counts.loc[keyword_counts.groupby(['month_year', 'label'])['counts'].idxmax()].reset_index(drop=True)

    # Calculate total counts for each month-year across all labels
    total_counts_all = unique_claims.groupby(['month_year']).size().reset_index(name='counts')
    total_counts_all['label'] = 'ALL'

    # Find the most recurrent entity for each month-year across all labels
    keyword_counts_all = df_filtered.groupby(['month_year', 'keywords']).size().reset_index(name='counts')
    most_recurrent_keyword_all = keyword_counts_all.loc[keyword_counts_all.groupby(['month_year'])['counts'].idxmax()].reset_index(drop=True)
    most_recurrent_keyword_all['label'] = 'ALL'

    # Merge total counts with most recurrent keyword info for each label
    merged_data = pandas.merge(total_counts, most_recurrent_keyword, on=['month_year', 'label'], suffixes=('', '_most_recurrent'))

    # Combine total counts and most recurrent keyword info for 'ALL' label
    merged_data_all = pandas.merge(total_counts_all, most_recurrent_keyword_all, on=['month_year', 'label'], suffixes=('', '_most_recurrent'))
    merged_data = pandas.concat([merged_data, merged_data_all], ignore_index=True)
    merged_data['popularity_percentage'] = (merged_data['counts_most_recurrent'] / merged_data['counts']) * 100
    merged_data['popularity_percentage'] = merged_data['popularity_percentage'].round(2)

    # Convert 'month_year' to string format for JSON serialization
    merged_data['month_year'] = merged_data['month_year'].astype(str)
    merged_data.rename(columns={'month_year': 'date1'}, inplace=True)

    data = merged_data.to_dict(orient='records')
    return jsonify(data)


def search_label_graph(filtered_df): 
    grouped_df = filtered_df.groupby('label').size().reset_index(name='counts')
    grouped_df['label'] = grouped_df['label'].str.upper()
    data = grouped_df.to_dict(orient='records')
    return jsonify(data)

def search_source_graph(filtered_df): 
    grouped_df = filtered_df.groupby(['source', 'label']).size().reset_index(name='counts')
    grouped_df['label'] = grouped_df['label'].str.upper()
    #Sort
    source_totals = grouped_df.groupby('source')['counts'].sum().reset_index(name='total_counts')
    sorted_sources = source_totals.sort_values(by='total_counts', ascending=False)['source']
    sorted_grouped_df = grouped_df.set_index('source').loc[sorted_sources].reset_index()

    data = sorted_grouped_df.to_dict(orient='records')
    return jsonify(data)

def search_language_graph(filtered_df): 
    grouped_df = filtered_df.groupby(['reviewBodyLang', 'label']).size().reset_index(name='counts')
    data = grouped_df.to_dict(orient='records')
    return jsonify(data)

def search_entity_graph(filtered_df):
      # Group by entity and count occurrences, then get the top 50 entities
    top_entities_df = (
        filtered_df['entity']
        .value_counts()
        .reset_index(name='counts')
        .rename(columns={'index': 'entity'})
        .head(50)
    )
    data = top_entities_df.to_dict(orient='records')
    return jsonify(data)

def compare_entity_graph(df_1, df_2):

    # Get entities from both dataframes and count occurrences
    df_1_counts = df_1['entity'].value_counts().reset_index(name='counts')
    df_1_counts = df_1_counts.rename(columns={'index': 'entity'})

    df_2_counts = df_2['entity'].value_counts().reset_index(name='counts')
    df_2_counts = df_2_counts.rename(columns={'index': 'entity'})

    # Merge the counts on 'entity' to get entities present in both dataframes
    merged_counts = pandas.merge(df_1_counts, df_2_counts, on='entity', suffixes=('_df1', '_df2'))

    # Sum the counts from both dataframes
    merged_counts['total_counts'] = merged_counts['counts_df1'] + merged_counts['counts_df2']

    # Get the top 50 entities based on the total counts
    top_entities_df = merged_counts.sort_values(by='total_counts', ascending=False).head(50)

    # Select only the entity and total_counts columns for the final result
    top_entities_df = top_entities_df[['entity', 'total_counts']]

    data = top_entities_df.to_dict(orient='records')
    return jsonify(data)
