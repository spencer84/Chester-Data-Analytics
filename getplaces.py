import requests
import json
import time
import pandas as pd

### Retrieve OS Maps API Key
### Use .gitignore to hide JSON file
with open('API Key.json') as f:
    api_key = json.loads(f.read())
key = api_key[0]['api_key']
url = 'https://api.os.uk/search/places/v1/postcode'  # URL for the Places API

### Set the postcode be queried
postcode = 'CH1'
# Define a function to query the OS Places API and paginate through all results

def postcode_query(url, key, postcode, offset):
    response = requests.get(url, params={'key': key, 'postcode': postcode, 'offset': offset})
    print(response.status_code)
    if response.status_code == 200:
        return response.json()
    elif response.status_code == 429:
        print('Status 429: Too many requests. Waiting 5 minutes...')
        time.sleep(300)
        postcode_query(url, key, postcode, offset)
    # else:
    #     print(f"Query failed: {response.status_code}")
    #     return


def full_query_postcode(url, key, postcode, throttle = True):
    """ Returns the full results of a postcode query to the Ordnance Survey Places API as a dictionary file
    :param url: API url
    :param key: Secret key for the OS API
    :param postcode: Any UK Postcode or the first part (district portion)
    :param throttle: If True, then adds a few seconds of time between each data get request to avoid a 429 code
    :return: Dictionary of query results
    """
    full_results = {}  # Initialise a dictionary to to store results through pagination
    count = 0
    response = postcode_query(url, key, postcode, count)  # Make a first initial query to get the first 100 results
    total_results = response['header']['totalresults']  # Identify total results of query
    print(total_results)
    while count <= total_results:
        for i in response['results']:
            full_results[count] = i  # Add each result to dictionary; 'count' is acting as the index
            count += 1  # Increment the count for each result added
            if count % 100 == 0:
                print(count)
        if throttle:
            time.sleep(3)
        if count == total_results:
            break
        response = postcode_query(url, key, postcode, count)  # Set the offset then re-run fo next 100 results
    return full_results
results = full_query_postcode(url, key, postcode)

### Write this result dictionary to a csv file
df = pd.DataFrame.from_dict({(i,j): results[i][j]
                           for i in results.keys()
                           for j in results[i].keys()},
                       orient='index')
### Write the dataframe to a CSV file
df.to_csv(postcode+'.csv')
