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
    response = requests.get(url, params={'postcode':postcode,'size':5000}, headers={ 'Authorization' : 'Basic %s' % key, "Accept":'text/csv'})
    response_content = response.content.decode('utf-8')
    cr = csv.reader(response_content.splitlines(), delimiter=',')
    csvfile = open('epc_' + postcode + '.csv', 'w', encoding='UTF8', newline='')
    writer = csv.writer(csvfile)
    writer.writerows(cr)


# Need to re-write this so that the query pulls all results, not just the first page of results
results = []
results_not_reached = True
total_count = 0
while results_not_reached:
    response = requests.get(url, params={'postcode': postcode, 'size': 5000,'from':total_count}, headers={'Authorization': 'Basic %s' % key, "Accept": 'json'})
    count = 0
    for i in response.json()['results']:
        results.append()
        count += 1
    if count < 5000: # The idea here is that if the response
        results_not_reached = False
    total_count += count
results = pd.DataFrame(results) # This format obviously won't work, but once the loop breaks, convert the parsed ...
# results to a Pandas dataframe which can then easily be written to a file.
return results
# Select postcode to search by
postcode = 'CH1'

# Call the function
get_postcode_data(encoded_api_key,postcode)


response = requests.get(url, params={'postcode':postcode}, headers={ 'Authorization' : 'Basic %s' % encoded_api_key, "Accept":'text/csv'})