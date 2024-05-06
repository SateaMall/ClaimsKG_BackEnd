from pathlib import Path
import pandas

pandas.set_option('display.max_colwidth', None)
pandas.set_option('display.max_columns', None)
base_path = Path(__file__).parent.parent
file_path = (base_path / "df_complete.csv").resolve()
df_complete = pandas.read_csv(file_path, dtype={"id1": str, "id2": str, "entity": str}, header=0)

pandas.set_option('display.max_colwidth', None)
pandas.set_option('display.max_columns', None)
base_path = Path(__file__).parent.parent
file_path = (base_path / "df_other.tsv").resolve()
df_other = pandas.read_csv(file_path, delimiter='\t', header=0)


