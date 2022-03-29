import getepcdata as epc
import getpricepaid as land
import sqlite3
import datetime

# Define path for API keys JSON file
path = 'API Key.json'
# Create a class for a property
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
        cur.execute("""SELECT MAX(date) FROM (SELECT * FROM data_log WHERE postcode_district = 'CH1' 
        AND data_table = 'epc')""")
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
            print("No data exists for this postcode district. Getting EPC Data...")
            land.get_full_price_paid(epc.get_key(path), self.postcode_district)
    def return_cursor(self):
        con = sqlite3.connect(self.db)
        cur = con.cursor()
        return cur



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