from pathlib import Path
import pandas

pandas.set_option('display.max_colwidth', -1)
pandas.set_option('display.max_columns', None)
base_path = Path(__file__).parent.parent
file_path = (base_path / "df_complete.csv").resolve()
df_complete = pandas.read_csv(file_path, dtype={"id1": str, "id2": str, "entity": str}, header=0)

df_Source_labelTRUE = pandas.read_csv('modules/df_Source_labelTRUE.csv', dtype={"id1": str, "id2": str, "entity": str},
                                      header=0)
df_Source_labelFALSE = pandas.read_csv('modules/df_Source_labelFALSE.csv',
                                       dtype={"id1": str, "id2": str, "entity": str}, header=0)
df_Source_labelOTHER = pandas.read_csv('modules/df_Source_labelOTHER.csv',
                                       dtype={"id1": str, "id2": str, "entity": str}, header=0)
df_Source_labelMIXTURE = pandas.read_csv('modules/df_Source_labelMIXTURE.csv',
                                         dtype={"id1": str, "id2": str, "entity": str}, header=0)
