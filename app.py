from flask import Flask
from modules.dataframes import generate_global_dataframe
from modules.dataframes import generate_per_label_dataframe
app = Flask(__name__)

@app.route ("/update_label_df")
def update_label_df():
    return generate_per_label_dataframe()

@app.route ("/update_global_df")
def update_global_df():
    return generate_global_dataframe()


if __name__ == '__main__':
    app.run(debug=True)
