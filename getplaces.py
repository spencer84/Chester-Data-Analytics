import requests
import json

### Need to get OS Maps API Key
### Store in git ignore file?

with open('API Key.json') as f:
    api_key = json.loads(f.read())
key = api_key[0]['api_key']
url = 'https://api.os.uk/search/places/v1/postcode'

response = requests.get(url,params ={'key':key,'postcode':'CH1'})
