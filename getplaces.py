import requests
import json
import time


### Retrieve OS Maps API Key
### Use .gitignore to hide JSON file
with open('API Key.json') as f:
    api_key = json.loads(f.read())
key = api_key[0]['api_key']
url = 'https://api.os.uk/search/places/v1/postcode'  # URL for the Places API

# Define a function to query the OS Places API and paginate through all results

def postcode_query(url, key, postcode, offset= 0):
    response = requests.get(url, params={'key': key, 'postcode': postcode, 'offset': offset})
    print(response.status_code)
    if response.status_code == 200:
        return response.json()
    elif response.status_code == 429:
        print('Status 429: Too many requests. Waiting 1 minute.')
        time.sleep(60)
        postcode_query(url, key, postcode, offset)
    # else:
    #     print(f"Query failed: {response.status_code}")
    #     return

def full_query_postcode(url, key, postcode):
    """ Returns the full results of a postcode query to the Ordnance Survey Places API as a dictionary file
    :param url: API url
    :param key: Secret key for the OS API
    :param postcode: Any UK Postcode or the first part (district portion)
    :return: Dictionary of query results
    """
    full_results = {}  # Initialise a dictionary to to store results through pagination
    count = 0
    response = postcode_query(url, key, postcode) # Make a first initial query to get the first 100 results
    total_results = response['header']['totalresults']  # Identify total results of query
    print(total_results)
    while count <= total_results:
        for i in response['results']:
            full_results[count] = i  # Add each result to dictionary; 'count' is acting as the index
            count += 1  # Increment the count for each result added
            if count % 100==0:
                print(count)
        response = postcode_query(url, key, postcode, offset= count)  # Set the offset then re-run fo next 100 results
    return full_results
results_test = full_query_postcode(url,key, 'CH1')
total_results_test = results_test['header']['totalresults']


print("Total results:",total_results_test)


### Need to write the results to a database

# This should run successfully...
test_dict = {}
test_count = 0
while test_count < 300:
    results_test = requests.get(url, params={'key': key, 'postcode': 'CH1', 'offset': test_count}).json()['results']
    print('Calling query...')
    for i in range(0,100):
        test_count += 1
        test_dict[test_count] = results_test[i]['DPA']
        print(test_count)