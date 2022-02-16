import requests
import pandas as pd

url = 'https://landregistry.data.gov.uk/data/ppi/transaction-record.json?'
properties = 'transactionId,transactionDate,pricePaid,propertyAddress.paon,propertyAddress.street'

town = 'CHESTER'
params_lr = {'_pageSize':200,
                     '_view':'basic',
                     '_properties':properties,
                     'propertyAddress.town':town}
def price_paid_query(params_lr):
    response = requests.get(url, params=params_lr)
    return response

## Retrive data
data = price_paid_query(params_lr)

## Parse data to get a dataframe of the results
data = data.json()['result']['items']

## Convert json to dataframe
df = pd.json_normalize(data)

## Change column names
col_names = ['url', 'Price Paid','Transaction Date', 'Transaction Id', 'Type', 'About','PAON', 'Street Name']
df.columns = col_names

### Changed to query all results from a given town

## Need to paginate through all results -- how to find out how many pages?
