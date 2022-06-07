import sqlite3
import getepcdata
import getpricepaid

conn = sqlite3.connect('cda.db')
cur = conn.cursor()

# Create a merged table in the CDA database

cur.execute("""CREATE TABLE merged AS select distinct * from land_reg
         inner join epc 
         on land_reg.postcode = epc.postcode 
         and land_reg.PAON like '%' || epc.address1|| '%'""")

# How to handle duplicate records (same property, different transactions)?
# Getting multiple land reg records, but not distinct because of timestamp
# Need to improve the initial upload process?
# Delete older transactions

cur.execute("""DELETE FROM merged
WHERE transaction_date < (
SELECT MAX(transaction_date) FROM merged t2 WHERE merged.postcode = t2.postcode
AND merged.PAON = t2.PAON
) 
""")

# Program to update the database and perform all data transactions
def update_(conn, postcode_area):
    """Update the land registry table with records for a given postcode"""
