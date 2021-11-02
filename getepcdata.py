import requests
import json

### Retrieve OS Maps API Key
### Use .gitignore to hide JSON file
with open('API Key.json') as f:
    api_key = json.loads(f.read())
key = api_key[0]['epc']
url = 'https://api.os.uk/search/places/v1/postcode'  # URL for the Places API