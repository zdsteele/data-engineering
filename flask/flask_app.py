from flask import Flask, jsonify, render_template
import pandas as pd

app = Flask(__name__)

@app.route("/")
def home():
    return render_template("index.html")

@app.route('/gold-data')
def gold_data():
    # Load the gold layer data (replace with your actual gold layer file path)
    df_gold = pd.read_csv('/app/data/gold_layer.csv')  # Update the path if needed

    # Convert the DataFrame to a list of dictionaries
    data = df_gold.to_dict(orient='records')
    return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
    print(f"Flask app is running on: http://127.0.0.1:5000")
