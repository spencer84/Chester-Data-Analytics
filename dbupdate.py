import sqlite3
import getepcdata
import getpricepaid

conn = sqlite3.connect('cda.db')
cur = conn.cursor()

# Create a merged table in the CDA database

cur.execute("""CREATE TABLE merged AS select * from land_reg
         inner join epc 
         on land_reg.postcode = epc.postcode 
         and land_reg.PAON like '%' || epc.address1|| '%'""")

# Program to update the database and perform all data transactions
def update_(conn, postcode_area):
    """Update the land registry table with records for a given postcode"""
