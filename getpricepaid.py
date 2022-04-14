import requests
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
params_lr = {'_pageSize': 200,
             '_view': 'basic',
             '_properties': properties,
             'propertyAddress.town': town}


def price_paid_query(cur, params_lr):
    """
    Queries the Land Registry API and with the
    :param cur: DB Cursor Object
    :param params_lr: Parameters for querying the Land Registry API
    :return: Length of results
    """
    response = requests.get(url, params=params_lr)
    results = response.json()['result']['items']
    # Need to write these results to the database
    curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # Iterate through the results dataframe, adding each value to the DB
    for i in results:
        vals_to_insert = [params_lr['propertyAddress.town'], None, None, None, None, i['transactionDate']
            , i['pricePaid'], curr_time]
        property_details = i['propertyAddress']
        # KeyError thrown if a given key isn't returned; Need to check before assigning value
        if 'postcode' in property_details:
            vals_to_insert[1] = get_postcode_district(i['propertyAddress']['postcode'])
            vals_to_insert[2] = i['propertyAddress']['postcode']
        if 'paon' in property_details:
            vals_to_insert[3] = i['propertyAddress']['paon']
        if 'street' in property_details:
            vals_to_insert[4] = i['propertyAddress']['street']

        cur.execute("INSERT INTO land_reg VALUES(?,?,?,?,?,?,?,?)"
                    , tuple(vals_to_insert))
    return len(results)

## Recursively paginate through results

def get_full_price_paid(params_lr, con, page=0):
    cur = con.cursor()
    params_lr['_page'] = page
    len_results = price_paid_query(cur, params_lr)
    print(len_results)
    while len_results == 200:  # Recursion until a page length less than max achieved
        page += 1
        print(page)
        params_lr['_page'] = page
        len_results = price_paid_query(cur, params_lr)
        con.commit()
        ## For some API results, there are no values resulting in a KeyError

        # If less than 200 results, then log the transaction and close the connection
    curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cur.execute("INSERT INTO data_log VALUES(?,?,?)", (params_lr['propertyAddress.town'], 'land_reg', curr_time))
    con.commit()
    con.close()
    return
