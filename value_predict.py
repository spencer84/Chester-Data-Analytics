from flask import Flask, redirect, request, url_for, render_template
from house_value_estimation  import Property
# import house_value_estimation
app = Flask(__name__)


@app.route('/')
def home():
    return render_template("index.html")


@app.route('/predict', methods=['GET', 'POST'])
def predict():
    """Predict the value of a given property using the inputed values."""
    if request.method == 'POST':
        postcode = request.form.get('postcode')
        paon = request.form.get('paon')
        town = request.form.get('town')
        prop = Property()
        prop.postcode = postcode
        prop.paon = paon
        prop.town = town
        # Calculate value
        if prop.validate_postcode():
            prop.check_for_model()
            prop.check_features()
            value = prop.predict()
        else:
            return """<p> Postcode not valid! </p> <a <href='/predict'> Try again </a>. """

        return f"""<p> The predicted value of {paon} {postcode} is £{value}</p> <"""
    elif request.method == 'GET':
        return render_template('predict.html')


# @app.route('/result/<postcode>')


if __name__ == '__main__':
    app.run(debug=True)
