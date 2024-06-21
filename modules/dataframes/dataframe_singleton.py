from pathlib import Path
import pandas

pandas.set_option('display.max_colwidth', None)
pandas.set_option('display.max_columns', None)
base_path = Path(__file__).parent.parent


file_path_entity = (base_path / "df_entity.csv").resolve()
df_entity = pandas.read_csv(file_path_entity, dtype={"id1": str, "id2": str, "entity": str}, header=0)

file_path_simple = (base_path / "df_simple.csv").resolve()
df_simple = pandas.read_csv(file_path_simple, dtype={"id1": str, "id2": str}, header=0)

file_path_keyword = (base_path / "df_keyword.csv").resolve()
df_keyword = pandas.read_csv(file_path_keyword, dtype={"id1": str, "id2": str, "keywords": str}, header=0)

file_path_topic = (base_path / "df_topic.tsv").resolve()
df_topic = pandas.read_csv(file_path_topic, delimiter='\t', header=0,encoding='latin-1')

#Pretreatements:
df_topic['entity'] = df_topic['entity'].fillna('').astype(str)
df_topic['entity'] = df_topic['entity'].apply(lambda x: x.split('/')[-1].replace('_', ' ') if isinstance(x, str) and x else '')
