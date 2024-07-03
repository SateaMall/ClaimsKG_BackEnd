from pathlib import Path
import pandas
# Set pandas options

pandas.set_option('display.max_colwidth', None)
pandas.set_option('display.max_columns', None)

# Define base path
base_path = Path(__file__).parent.parent

# Function to read a CSV,TSV file if it exists and is not empty, otherwise return an empty DataFrame
def read_csv_if_exists(file_path, **kwargs):
    if file_path.exists():
        if file_path.stat().st_size > 0:  # Check if file is not empty
            return pandas.read_csv(file_path, **kwargs)
        else:
            print(f"File {file_path} is empty. Initializing an empty DataFrame.")
            return pandas.DataFrame()
    else:
        print(f"File {file_path} does not exist. Initializing an empty DataFrame.")
        return pandas.DataFrame()

file_path_entity = (base_path / "df_entity.csv").resolve()
file_path_simple = (base_path / "df_simple.csv").resolve()
file_path_keyword = (base_path / "df_keyword.csv").resolve()
file_path_topic = (base_path / "df_topic.tsv").resolve()

# Read CSV files 
df_entity = read_csv_if_exists(file_path_entity, dtype={"id1": str, "id2": str, "entity": str}, header=0)
df_simple = read_csv_if_exists(file_path_simple, dtype={"id1": str, "id2": str}, header=0)
df_keyword = read_csv_if_exists(file_path_keyword, dtype={"id1": str, "id2": str, "keywords": str}, header=0)
df_topic = read_csv_if_exists(file_path_topic, delimiter='\t', header=0, encoding='latin-1')

# Pretreatments for df_topic if it is not empty
if not df_topic.empty:
    df_topic['entity'] = df_topic['entity'].fillna('').astype(str)
    df_topic['entity'] = df_topic['entity'].apply(lambda x: x.split('/')[-1].replace('_', ' ') if isinstance(x, str) and x else '')

