from flask import Flask, redirect, request, url_for, render_template
from house_value_estimation import Property, get_postcode_district
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
        prop.number = paon
        prop.town = town
        prop.postcode_district = get_postcode_district(prop.postcode)
        # Calculate value
        if prop.validate_postcode():
            prop.check_for_model()
            prop.check_features()
            value = prop.predict()
            model_details = prop.model_perf
        else:
            return """<p> Postcode not valid! </p> <a <href='/predict'> Try again </a>. """

        return f"""<p> The predicted value of {paon} {postcode} is £{value}</p> 
        <p> Model Type: Linear Regression </p>
        <p> Coefficients: {model_details['Coefficients']} </p>
        <p> Mean squared error: {model_details['Mean squared error']}  </p>"""
    elif request.method == 'GET':
        return render_template('predict.html')


# @app.route('/result/<postcode>')


if __name__ == '__main__':
    app.run(debug=True)
