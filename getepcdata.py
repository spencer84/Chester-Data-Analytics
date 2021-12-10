import requests
import json
import base64
import pandas as pd

# Retrieve OS Maps API Key
# Use .gitignore to hide JSON file
with open('API Key.json') as f:
    api_key = json.loads(f.read())
key = api_key['epc_key']
username = api_key['epc_username']
url = 'https://epc.opendatacommunities.org/api/v1/domestic/search'  # URL for the Places API

# Need to encode the username and key then strip off the extra bits
encoded_api_key = str(base64.b64encode(bytes(username + ':' + key, 'utf-8')))[1:].replace('\'', "")


# encoded_api_key = 'c2FtOTVAdnQuZWR1OjkzMzUyZDQ2N2I5YjU3YmZkNTNmZjBkMGQ1NzFlNjVhMjUyOWZiMzc='
# 'Accept':'application/json'


def get_postcode_data(key, postcode):
    """" Returns a pandas dataframe containing all results for the given postcode.
    This can be written to a CSV file for later processing.
    :param key: Encoded secret key for the OS API
    :param postcode: Any UK Postcode or the first part (district portion)
    :return: Pandas Dataframe of EPC results
    """
    # Can only paginate through the first 10,000 results of any query. Need to use energy bands (or other parameter)
    # to break up the query.
    energy_bands = ['a', 'b', 'c', 'd', 'e', 'f', 'g']
    results = []
    for band in energy_bands:
        results_not_reached = True
        total_count = 0
        while results_not_reached:
            response = requests.get(url, params={'postcode': postcode, 'size': 5000, 'from': total_count,
                                                 'energy-band': band},
                                    headers={'Authorization': 'Basic %s' % key, "Accept": 'application/json'})
            results += response.json()['rows']
            count = len(response.json()['rows'])
            if count < 5000:  # The idea here is that if the response
                results_not_reached = False
            total_count += count
    results = pd.DataFrame(data=results, columns=response.json()['column-names'])
    return results


# Select postcode to search by
postcode = 'CH1'

# Call the function
results = get_postcode_data(encoded_api_key, postcode)

# Write to CSV
results.to_csv(postcode + '.csv')
