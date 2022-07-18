from flask import Flask, redirect
import house_value_estimation
app = Flask(__name__)

@app.route('/predict', methods = ['POST'])
def predict():
    if request.methods = 'POST':

