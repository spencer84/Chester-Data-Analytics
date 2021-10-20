import requests
import json


### Retrieve OS Maps API Key
### Use .gitignore to hide JSON file

with open('API Key.json') as f:
    api_key = json.loads(f.read())
key = api_key[0]['api_key']
url = 'https://api.os.uk/search/places/v1/postcode'

# Define a function to query the OS Places API and paginate through all results

def postcode_query(url, key, postcode):
    response = requests.get(url, params={'key': key, 'postcode': postcode})
    return response.json()

def full_query(url, key, postcode):
    full_results = {}  # Initialise a dictionary to to store results through pagination
    count = 0
    response = postcode_query(url, key, postcode)
    total_results = response.json()['header']['totalresults']  # Identify total results of query
    while count < total_results:
        for i in range(0,100):
            full_results[i] = response[i]['DPA']
            count += 1
        response = postcode_query(url, key, postcode)

response = requests.get(url,params ={'key':key,'postcode':'CH1'})


response['totalresults']