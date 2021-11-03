import requests
import json
import base64

### Retrieve OS Maps API Key
### Use .gitignore to hide JSON file
with open('API Key.json') as f:
    api_key = json.loads(f.read())
key = api_key[0]['epc_key']
username = api_key[0]['epc_username']
url = 'https://epc.opendatacommunities.org/api/v1/domestic/search'  # URL for the Places API
encoded_api_key = base64.b64encode(bytes(username + ':' + key, 'utf-8'))

response = requests.get(url, params={}, headers={ 'Authorization' : 'Basic %s' % encoded_api_key,'Accept':'application/json'})