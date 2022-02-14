import requests

url = 'https://landregistry.data.gov.uk/data/ppi/transaction-record.json?'
properties = 'transactionId,transactionDate,pricePaid,propertyAddress.paon,propertyAddress.street'

test_post_code = 'CH1 1HD'
params_lr = {'_pageSize':200,
                     '_view':'basic',
                     '_properties':properties,
                     'propertyAddress.postcode':test_post_code}
def price_paid_query(params_lr):
    response = requests.get(url, params=params_lr)
    return response

test = price_paid_query(params_lr)

test.json()