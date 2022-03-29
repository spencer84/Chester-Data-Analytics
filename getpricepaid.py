import requests
import pandas as pd

url = 'https://landregistry.data.gov.uk/data/ppi/transaction-record.json?'
properties = 'transactionId,transactionDate,pricePaid,propertyAddress.paon,propertyAddress.street,propertyAddress.postcode'

town = 'CHESTER'
params_lr = {'_pageSize':200,
                     '_view':'basic',
                     '_properties':properties,
                     'propertyAddress.town':town}
def price_paid_query(params_lr):
    response = requests.get(url, params=params_lr)
    results = results.json()['result']['items']
    results = pd.DataFrame(data=results, columns=response.json()['column-names'])
    # Need to write these results to the database
    curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # Write to DB
    con = sqlite3.connect('cda.db')
    cur = con.cursor()
    # Iterate through the results dataframe, adding each value
    for index, row in results.iterrows():
        cur.execute("INSERT INTO epc VALUES(?,?,?,?,?,?,?,?)", (row.address, row.address1, row.uprn, row['postcode'],
                                                                row['current-energy-rating'], row['total-floor-area'],
                                                                row['lodgement-datetime'], curr_time))
    return response

## Retrive data
#data = price_paid_query(params_lr)

## Parse data to get a dataframe of the results
#data = data.json()['result']['items']

## Convert json to dataframe
#df = pd.json_normalize(data)

## Recursively paginate through results

page = 0
df = pd.DataFrame()

def get_full_price_paid(df, page, params_lr):
    data = price_paid_query(params_lr)
    data = data.json()['result']['items']
    params_lr['_page'] = page
    df = pd.concat([df, pd.json_normalize(data)])
    if len(data) == 200: # Recursion until a page length less than max achieved
        page += 1
        df = get_full_price_paid(df, page, params_lr)
    # When running the code the length appears to increase
    # But the final output only yields 200 results?
    return df

df = get_full_price_paid(df, page, params_lr)

## Change column names
col_names = ['url', 'Price Paid','Transaction Date', 'Transaction Id', 'Type', 'About','PAON', 'Postcode', 'Street Name']
df.columns = col_names

## Output
df.to_csv('price_paid_'+town+'.csv')

## Paginating through the results for an entire town works, but is slow
## Is there a way to find out if a postcode contains a district?

# Need to do some processing to identify a given postcode
df['postcode district'] = df['Postcode'].apply(lambda x: str(x)[:3])
