import requests
import json


### Retrieve OS Maps API Key
### Use .gitignore to hide JSON file
with open('API Key.json') as f:
    api_key = json.loads(f.read())
key = api_key[0]['api_key']
url = 'https://api.os.uk/search/places/v1/postcode' # URL for the Places API

# Define a function to query the OS Places API and paginate through all results

def postcode_query(url, key, postcode):
    response = requests.get(url, params={'key': key, 'postcode': postcode})
    return response.json()

def full_query_postcode(url, key, postcode):
    """ Returns the full results of a postcode query to the Ordnance Survey Places API as a dictionary file
    :param url: API url
    :param key: Secret key for the OS API
    :param postcode: Any UK Postcode or the first part (district portion)
    :return: Dictionary of query results
    """
    full_results = {}  # Initialise a dictionary to to store results through pagination
    count = 0
    response = postcode_query(url, key, postcode)
    total_results = response.json()['header']['totalresults']  # Identify total results of query
    while count < total_results:
        for i in range(0,100):
            full_results[i] = response[i]['DPA'] # Add each result to dictionary
            count += 1  # Increment the count for each result added
        response = requests.get(url, params={'key': key, 'postcode': postcode, 'offset': count)  # At the end of the first 100, re

results_test = postcode_query(url,key, 'CH1')
total_results_test = results_test['header']['totalresults']


print("Total results:",total_results_test)


### Need to write the results to a database

# This should run successfully...
while test_count <= 300:
    results_test = requests.get(url, params={'key': key, 'postcode': 'CH1', 'offset': test_count}).json()['results']
    for i in range(0,100):
        test_dict[i] = results_test[i]['DPA']
        test_count =+ 1