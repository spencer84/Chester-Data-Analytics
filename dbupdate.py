import sqlite3
import getepcdata
import getpricepaid

conn = sqlite3.connect('cda.db')
cur = conn.cursor()

# Create a merged table in the CDA database

cur.execute("""CREATE TABLE recent_land as 
select *, max(transaction_date) as most_recent_transaction from land_reg group by paon, postcode""")

cur.execute("""CREATE TABLE merged AS (recent_land
         inner join epc 
         on recent_land.postcode = epc.postcode 
         and recent_land.PAON like '%' || epc.address1|| '%')""")


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
