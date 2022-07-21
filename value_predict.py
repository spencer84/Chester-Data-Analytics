from flask import Flask, redirect, request, url_for, render_template

# import house_value_estimation
app = Flask(__name__)


@app.route('/')
def home():
    return render_template("index.html")


@app.route('/predict', methods=['GET', 'POST'])
def predict():
    if request.method == 'POST':
        postcode = request.form.get('postcode')
        paon = request.form.get('paon')
        return f"""<p> {postcode} </p> <"""
    elif request.method == 'GET':
        return render_template('predict.html')


# @app.route('/result/<postcode>')


if __name__ == '__main__':
    app.run(debug=True)
