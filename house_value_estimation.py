import getepcdata as epc
import getpricepaid as land
import sqlite3
import datetime
import pandas as pd

# Define path for API keys JSON file
path = 'API Key.json'
# Create a class for a property

def sql_query_to_df(cur, query, table):
    """
    Construct a Pandas DataFrame from a given SQL table and query. The PRAGMA table_info() section returns the columns
    used in the table, then the results from the query are parsed into individual arrays, then combined.
    :param cur: The cursor object created by the database connection
    :param query: The query that the resulting dataframe is based on
    :param table: The table within the database that is being queried
    :return:
    """
    # First get the colunmns
    cols = []
    # Need to find better placeholder
    cur.execute("PRAGMA table_info(?)",(table,))
    column_names = cur.fetchall()
    for i in column_names:
        cols.append(i[1])
    cur.execute(query)
    rows = cur.fetchall()
    col_dict = {}
    for i in cols:
        col_dict[i] = []
        for j in rows:
            col_dict[i].append(j[cols.index(i)])
    return pd.DataFrame(col_dict)

class Property:
    def __init__(self):
        self.postcode = None
        self.number = None
        self.street = None
        self.full_address = None
        self.db = 'cda.db'
        self.postcode_district = None
        self.town = None
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
        self.postcode_district = self.postcode.split(" ")[0]
        town = input("Town:")
        self.town = town
    def check_postcode_data(self):
        # Connect to db
        con = sqlite3.connect(self.db)
        cur = con.cursor()
        cur.execute("SELECT * FROM data_log WHERE postcode_district =? ", (self.postcode_district,))
        results = cur.fetchall()
        # Find most recent EPC records
        cur.execute("""SELECT MAX(date) FROM (SELECT * FROM data_log WHERE postcode_district =?  
        AND data_table = 'epc'""",(self.postcode_district,))
        max_epc = cur.fetchall()
        if len(max_epc) == 0:
            print("No data exists for this postcode district. Getting EPC Data...")
            epc.get_postcode_epc_data(epc.get_key(path), self.postcode_district)
        else:
            epc_age = datetime.datetime.today() - datetime.datetime.fromisoformat(max_epc[0][0])
            if epc_age > 30:
                update_epc = input("EPC Data is more than 30 days old. Update? y/n")
                if update_epc == 'y':
                    epc.get_postcode_epc_data(epc.get_key(path), self.postcode_district)
                elif update_epc == 'n':
                    pass
                else:
                    print("Input not understood...Data will not be updated.")
        # Find most recent land_reg records
        params_lr = {'_pageSize': 200,
                     '_view': 'basic',
                     '_properties': properties,
                     'propertyAddress.town': self.town}
        cur.execute("""SELECT MAX(date) FROM (SELECT * FROM data_log WHERE postcode_district = 'CH1' 
        AND data_table = 'land_reg')""")
        max_land_reg = cur.fetchall()
        if len(max_land_reg) == 0:
            print("No data exists for this postcode district. Getting Land Registry Data...")
            land.get_full_price_paid(epc.get_key(path), self.postcode_district)
    def return_cursor(self):
        con = sqlite3.connect(self.db)
        cur = con.cursor()
        return cur
    def query_data(self):
        """Once data is updated and checked, create new attributes of the property object as Pandas DataFrames
        containing both the relevant data to create the prediction model
        """
        cur = self.return_cursor()
        cur.execute("SELECT * FROM epc WHERE postcode_district ?=",(self.postcode_district,))
        rows = cur.fetchall()
        ### How convert SQL query to Pandas Dataframe?

    def create_merged_table(self):
        merged_table = pd.merge(self.epc, self.land_reg, )
        self.merged_table = merged_table




# Identify property for estimate
prop = Property()
prop.postcode = 'CH1 1SD'
prop.postcode_district = 'CH1'
#prop.get_input()
prop.check_postcode_data()
cur = prop.return_cursor()




## Quickly create a new table to log data sources and track when updated
# test = 'CH2'
# import sqlite3
# con = sqlite3.connect('cda.db')
# cur = con.cursor()
#
# results  = cur.execute("SELECT * FROM data_log WHERE postcode_district =?",(test,))