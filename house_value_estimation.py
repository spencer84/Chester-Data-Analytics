import getepcdata as epc
import sqlite3

# Create a class for a property
class Property:
    def __init__(self):
        self.postcode = None
        self.number = None
        self.street = None
        self.full_address = None
        self.db = 'cda.db'
        self.postcode_district = None
    def get_input(self):
        """
        Get data from user
        :return: self, updated address attributes
        """
        postcode = input("Postcode:")
        self.postcode = postcode
        # Remove spaces from postcode
        number = input("House/Flat Number:")
        self.number = number
        self.postcode_district = self.postcode[:2]
    def check_postcode_data(self):
        # Connect to db
        con = sqlite3.connect(self.db)
        cur = con.cursor()
        results  = cur.execute("""SELECT *""")
        # Is there data from both the EPC and Land Registry within the last Month?


#
# Identify property for estimate
prop = Property()
prop.get_input()


## Quickly create a new table to log data sources and track when updated


con = sqlite3.connect('cda.db')
cur = con.cursor()
cur.execute("""CREATE TABLE data_log (postcode_district text, epc bool, epc_date text, land_reg bool, land_reg_date text)""")
