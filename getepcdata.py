import requests
import json
import base64
import csv

### Retrieve OS Maps API Key
### Use .gitignore to hide JSON file
with open('API Key.json') as f:
    api_key = json.loads(f.read())
key = api_key['epc_key']
username = api_key['epc_username']
url = 'https://epc.opendatacommunities.org/api/v1/domestic/search'  # URL for the Places API

# Need to encode the username and key then strip off the extra bits
encoded_api_key = str(base64.b64encode(bytes(username + ':' + key, 'utf-8')))[1:].replace('\'',"")
#encoded_api_key = 'c2FtOTVAdnQuZWR1OjkzMzUyZDQ2N2I5YjU3YmZkNTNmZjBkMGQ1NzFlNjVhMjUyOWZiMzc='
#,'Accept':'application/json'

def get_postcode_data(key, postcode):
    """" Writes a CSV containing all results for the given postcode to the local directory
    :param key: Encoded secret key for the OS API
    :param postcode: Any UK Postcode or the first part (district portion)
    :return: None
    """
    response = requests.get(url, params={'postcode':postcode}, headers={ 'Authorization' : 'Basic %s' % key, "Accept":'text/csv'})
    csvfile = open('epc_'+postcode + '.csv', 'w')
    writer = csv.writer(csvfile, delimiter=' ')
    for row in response.text.split('\n'):
        writer.writerow(row)

# Select postcode to search by
postcode = 'CH1'

# Call the function
get_postcode_data(encoded_api_key,postcode)


