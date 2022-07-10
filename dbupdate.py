import sqlite3
import getepcdata as epc
import datetime
import getpricepaid

conn = sqlite3.connect('cda.db')
cur = conn.cursor()


def update_all(postcode_area):
    """
    Update the data from both EPC records and
    :param postcode_area:
    :return:
    """
    # Update EPC records
    cur.execute("""SELECT MAX(date) FROM (SELECT * FROM data_log WHERE postcode_district = :postcode  
            AND data_table = 'epc' )""", {"postcode": postcode_area})
    max_epc = cur.fetchall()
    if max_epc[0] == (None,):
        print("No data exists for this postcode district. Getting EPC Data...")
        epc.get_postcode_epc_data(epc.get_key(path), postcode_area)
    else:
        epc_age = datetime.datetime.today() - datetime.datetime.fromisoformat(max_epc[0][0])
        if epc_age.days >= 7:
            print("EPC data is 1 week old. Updating records...")
            epc.get_postcode_epc_data(epc.get_key(path), postcode_area)
            print("EPC data updated.")
        else:
            pass
    # Update Land Registry Records



# Create a merged table in the CDA database

cur.execute("""CREATE TABLE recent_land as 
select *, max(transaction_date) as most_recent_transaction from land_reg group by paon, postcode""")

cur.execute("""CREATE TABLE merged AS select * from recent_land
         inner join epc 
         on recent_land.postcode = epc.postcode 
         and recent_land.PAON like '%' || epc.address1|| '%'""")


# Need to take only most recent record from land reg data

# How many unique combinations are being captured?

cur.execute("""select count(distinct address) from (select PAON || ' '|| postcode AS address from land_reg)""")

cur.execute("""select count(address) as address_count, address, transaction_date from (select PAON || ' '|| postcode AS address
            , transaction_date from land_reg) group by address order by address_count desc""")

land_reg_count = cur.fetchall()

cur.execute("select count(postcode) from merged")

merged_count = cur.fetchall()

# How to handle duplicate records (same property, different transactions)?
# Getting multiple land reg records, but not distinct because of timestamp
# Need to improve the initial upload process?
# Delete older transactions

# Sort transactions by age then delete
cur.execute("""SELECT
	*,
	RANK () OVER ( 
		PARTITION BY postcode + PAON
		ORDER BY transaction_date DESC
	) transaction_rank 
FROM
	merged
""")

ranked = cur.fetchall()


cur.execute("""DELETE FROM merged
WHERE transaction_date < (
SELECT MAX(transaction_date) FROM merged t2 WHERE merged.postcode = t2.postcode
AND merged.PAON = t2.PAON
) 
""")

# Program to update the database and perform all data transactions
def update_(conn, postcode_area):
    """Update the land registry table with records for a given postcode"""
