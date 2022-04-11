import requests
import pandas as pd
import datetime
import sqlite3

def get_postcode_district(postcode):
    """ Returns the postcode district/area from a given postcode
    """
    if ' ' in postcode:
        return postcode.split(' ')[0]
    elif len(postcode) == 6:
        return postcode[:3]
    else:
        return postcode[:4]



url = 'https://landregistry.data.gov.uk/data/ppi/transaction-record.json?'
properties = 'transactionId,transactionDate,pricePaid,propertyAddress.paon,propertyAddress.street,propertyAddress.postcode'

town = 'CHESTER'
params_lr = {'_pageSize':200,
                     '_view':'basic',
                     '_properties':properties,
                     'propertyAddress.town':town}

def price_paid_query(cur, params_lr):
    response = requests.get(url, params=params_lr)
    results = response.json()['result']['items']
    # Need to write these results to the database
    curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # Iterate through the results dataframe, adding each value to the DB
    for i in results:
        cur.execute("INSERT INTO land_reg VALUES(?,?,?,?,?,?,?,?)"
        , (params_lr['propertyAddress.town'], get_postcode_district(i['propertyAddress']['postcode'])
        , i['propertyAddress']['postcode'], i['propertyAddress']['paon']
        , i['propertyAddress']['street'], i['transactionDate'], i['pricePaid'], curr_time))
    return len(results)



## Retrive data
#data = price_paid_query(params_lr)

## Parse data to get a dataframe of the results
#data = data.json()['result']['items']

## Convert json to dataframe
#df = pd.json_normalize(data)

## Recursively paginate through results

def get_full_price_paid(params_lr, con, page = 0):
    cur = con.cursor()
    params_lr['_page'] = page
    len_results = price_paid_query(cur, params_lr)
    if len_results == 200: # Recursion until a page length less than max achieved
        page += 1
        params_lr['_page'] = page
        price_paid_query(cur, params_lr)
        con.commit()
    else:
        # If less than 200 results, then log the transaction and close the connection
        curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cur.execute("INSERT INTO data_log VALUES(?,?,?)", (params_lr['propertyAddress.town'], 'land_reg', curr_time))
        con.commit()
        con.close()
    return



con = sqlite3.connect('cda.db')
get_full_price_paid(params_lr, con)
#price_paid_query(cur, params_lr)
## Paginating through the results for an entire town works, but is slow
## Is there a way to find out if a postcode contains a district?


