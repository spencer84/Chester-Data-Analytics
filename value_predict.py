from flask import Flask, redirect, request, url_for
#import house_value_estimation
app = Flask(__name__)

@app.route('/predict.html', methods = ['GET','POST'])
def predict():
    if request.method == 'POST':
        print(request)
    else:
        return redirect(url_for('/predict.html'))


if __name__ == '__main__':
    app.run(debug=True)


