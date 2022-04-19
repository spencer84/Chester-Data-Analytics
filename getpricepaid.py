import requests
import datetime
import sqlite3
import queue

def get_postcode_district(postcode):
    """ Returns the postcode district/area from a given postcode
    """
    if ' ' in postcode:
        return postcode.split(' ')[0]
    elif len(postcode) == 6:
        return postcode[:3]
    else:
        return postcode[:4]

class LandData:
    def __init__(self):
        self.url = 'https://landregistry.data.gov.uk/data/ppi/transaction-record.json?'
        self.properties = 'transactionId,transactionDate,pricePaid,propertyAddress.' \
                           'paon,propertyAddress.street,propertyAddress.postcode'
        self.town = None
        self.page = 0
        self.results_len = 0
        self.params = {'_view': 'basic',
                       '_properties': self.properties,
                       '_pageSize': 200,
                       'propertyAddress.town': self.town,
                       '_page': self.page}
        self.status = None
        self.conn = None
        self.cur = None
        self.results_to_add = queue.Queue()
        self.request_status_code = None
        self.query_in_progress = False

    def create_connection(self, db):
        self.conn = sqlite3.connect(db)
        print("Connected to " + db)

    def create_cursor(self):
        self.cur = self.conn.cursor()

    def close_connection(self):
        self.conn.commit()
        self.conn.close()
        print("Connection closed")

    def price_paid_query(self):
        """
        Queries the Land Registry API and with the given 'town' attribute of the object. Updates the request_status_code
        and the results_len attributes
        """
        self.params = {'_view': 'basic',
                       '_properties': self.properties,
                       '_pageSize': 200,
                       'propertyAddress.town': self.town,
                       '_page': self.page}
        response = requests.get(self.url, params=self.params)
        self.request_status_code = response.status_code
        results = response.json()['result']['items']
        self.results_len = len(results)
        # Need to write these results to the database
        curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # Iterate through the results dataframe, adding each value to the DB
        for i in results:
            vals_to_insert = [self.town, None, None, None, None, i['transactionDate']
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
            # Add each line of results to the queue of values to be added to the database
            self.results_to_add.put(vals_to_insert)

    def get_full_price_paid(self):
        end_of_results = False
        while not end_of_results:  # Recursion until a page length less than max achieved
            self.price_paid_query()
            self.page += 1
            if self.results_len < 200:
                end_of_results = True
            print(self.page)
            # Write results to database
        print("Writing results to db")
        self.data_to_db()

    def data_to_db(self):
        while self.query_in_progress:
            self.cur.execute("")
        curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.cur.execute("INSERT INTO data_log VALUES(?,?,?)", (params_lr['propertyAddress.town'], 'land_reg', curr_time))
        self.conn.commit()
        self.conn.close()
        return


land = LandData()
land.town = 'CHESTER'
land.create_connection('cda.db')
land.create_cursor()
land.get_full_price_paid()


