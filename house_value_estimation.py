import getepcdata as epc
import getpricepaid as land
import sqlite3
import datetime
import pandas as pd

# Define path for API keys JSON file
path = 'API Key.json'


# Create a class for a property

def sql_query_to_df(cursor, query):
    """
    Construct a Pandas DataFrame from a given SQL table and query. The PRAGMA table_info() section returns the columns
    used in the table, then the results from the query are parsed into individual arrays, then combined.
    :param cursor: The cursor object created by the database connection
    :param query: The query that the resulting dataframe is based on
    :return: Pandas DataFrame of the query
    """
    # First get the columns
    cols = []
    # Need to find better placeholder
    cursor.execute(query)
    column_names = cursor.description
    for i in column_names:
        cols.append(i[0])
    cursor.execute(query)
    rows = cursor.fetchall()
    col_dict = {}
    for i in cols:
        col_dict[i] = []
        for j in rows:
            col_dict[i].append(j[cols.index(i)])
    return pd.DataFrame(col_dict)


def get_postcode_district(postcode):
    """ Returns the postcode district/area from a given postcode
    """
    if ' ' in postcode:
        return postcode.split(' ')[0]
    elif len(postcode) == 6:
        return postcode[:3]
    else:
        return postcode[:4]


class Property:
    def __init__(self):
        self.postcode = None
        self.number = None
        self.street = None
        self.full_address = None
        self.db = 'cda.db'
        self.postcode_district = None
        self.town = None
        self.merged_table = None
        self.epc_table = None
        self.land_reg_table = None

    def get_input(self):
        """
        Get data from user
        :return: self, updated address attributes
        """
        postcode = input("Postcode:")
        self.postcode = postcode
        # Remove spaces from postcode and sterilize to avoid SQL injection
        number = input("House/Flat Number:")
        self.number = number
        self.postcode_district = get_postcode_district(self.postcode)
        town = input("Town:")
        self.town = town

    def check_postcode_data(self):
        # Connect to db
        con = sqlite3.connect(self.db)
        cur = con.cursor()
        cur.execute("SELECT * FROM data_log WHERE postcode_district =? ", (self.postcode_district,))
        results = cur.fetchall()
        # Find most recent EPC records
        cur.execute("""SELECT MAX(date) FROM (SELECT * FROM data_log WHERE postcode_district = :postcode  
        AND data_table = 'epc' )""", {"postcode":self.postcode_district})
        max_epc = cur.fetchall()
        print(max_epc)
        if max_epc[0] == (None,):
            print("No data exists for this postcode district. Getting EPC Data...")
            epc.get_postcode_epc_data(epc.get_key(path), self.postcode_district)
        else:
            epc_age = datetime.datetime.today() - datetime.datetime.fromisoformat(max_epc[0][0])
            if epc_age.days > 30:
                update_epc = input("EPC Data is more than 30 days old. Update? y/n")
                if update_epc == 'y':
                    epc.get_postcode_epc_data(epc.get_key(path), self.postcode_district)
                elif update_epc == 'n':
                    pass
                else:
                    print("Input not understood...Data will not be updated.")
        # Find most recent land_reg records
        properties = 'transactionId,transactionDate,pricePaid,propertyAddress.paon,propertyAddress.street,' \
                     'propertyAddress.postcode'
        params_lr = {'_pageSize': 200,
                     '_view': 'basic',
                     '_properties': properties,
                     'propertyAddress.town': self.town}
        cur.execute("""SELECT MAX(date) FROM (SELECT * FROM data_log WHERE postcode_district =?
        AND data_table = 'land_reg')""", self.town)
        max_land_reg = cur.fetchall()
        print(max_land_reg)
        if max_land_reg[0] == (None,):
            print("No data exists for this postcode district. Getting Land Registry Data...")
            land.get_full_price_paid(params_lr,con)
        else:
            land_age = datetime.datetime.today() - datetime.datetime.fromisoformat(max_land_reg[0][0])
            if land_age.days > 30:
                update_land = input("Land Registry Data is more than 30 days old. Update? y/n")
                if update_land == 'y':
                    land.get_full_price_paid(params_lr,con)
                elif update_land == 'n':
                    pass
                else:
                    print("Input not understood...Data will not be updated.")

    def return_cursor(self):
        con = sqlite3.connect(self.db)
        cur = con.cursor()
        return cur

    def query_data(self):
        """Once data is updated and checked, create new attributes of the property object as Pandas DataFrames
        containing both the relevant data to create the prediction model
        """
        # Create EPC DataFrame
        epc_query = "SELECT * FROM epc WHERE postcode_district = " + str(self.postcode_district)
        self.epc_table = sql_query_to_df(self.return_cursor(), epc_query)
        # Create Land Registry Price Paid DataFrame
        land_reg_query = "SELECT * FROM land_reg WHERE postcode_district = " + str(self.town)
        self.land_reg_table = sql_query_to_df(self.return_cursor(), land_reg_query)

    def prep_for_merge(self):

        ### What needs to be done to faciliate the merging of these records?
        return

    # def create_merged_table(self):
    #     merged_table = pd.merge(self.epc_table, self.land_reg_table, how='left', on = )
    #     self.merged_table = merged_table
    #
    # def create_model(self):



# Identify property for estimate
prop = Property()
prop.postcode = 'CH1 1SD'
prop.postcode_district = 'CH1'
# prop.get_input()
prop.check_postcode_data()
cur = prop.return_cursor()
prop.query_data()
prop.create_merged_table()

# Quickly create a new table to log data sources and track when updated
# test = 'CH2'
# import sqlite3
# con = sqlite3.connect('cda.db')
# cur = con.cursor()
#
# results  = cur.execute("SELECT * FROM data_log WHERE postcode_district =?",(test,))
