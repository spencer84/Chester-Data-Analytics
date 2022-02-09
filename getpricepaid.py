import requests

url = 'http://landregistry.data.gov.uk/data/ppi/address.json'


def price_paid_query(postcode, street):
    response = requests.get(url, params={'street':street,'postcode': postcode})
    return response

test = price_paid_query('CH1', 'Abbots Nook')