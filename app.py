from flask import Flask
from modules.dataframes import generate_global_dataframe
from modules.dataframes import generate_per_label_dataframe
from modules.statistics.summary import dico_numbers_resume
app = Flask(__name__)

@app.route ("/update_label_df")
def update_label_df():
    return generate_per_label_dataframe()

@app.route ("/update_global_df")
def update_global_df():
    return generate_global_dataframe()

##TODO add rest api
@app.route ("/graph/perlabeldate")
def graph_per_label_date():
    return 5

@app.route ("/resume")
def resume():
    return dico_numbers_resume()



if __name__ == '__main__':
    app.run(debug=True)
