from flask import Flask, render_template, request, url_for
from SQL_to_pyspark_v2 import *
app = Flask(__name__,
static_url_path="/static")

@app.route("/", methods = ['POST', 'GET'])
def index():
    if request.method == "POST":
        return render_template("index.html", formdata = request.form, result = main(request.form['sql']))
    else:
        return render_template("index.html")

if __name__ == "__main__":
    app.run(debug=True)