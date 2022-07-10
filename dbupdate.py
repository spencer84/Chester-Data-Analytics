import sqlite3
import getepcdata as epc
import datetime
import getpricepaid as land

# Define path for API keys JSON file
path = 'API Key.json'

conn = sqlite3.connect('cda.db')
cur = conn.cursor()

def update_epc(cur,postcode_area, max_days = 7):
    """
    Update the EPC records for all postcodes in a given postcode area.
    :param postcode_area: The postcode area (prefix) to update
    :param max_days: Length of time in days before the data is updated.
    :return: None
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
        if epc_age.days >= max_days:
            print("EPC data is more than 1 week old. Updating records...")
            epc.get_postcode_epc_data(epc.get_key(path), postcode_area)
            print("EPC data updated.")

def update_land(cur, town, max_days = 7):
    """
    Update the Land Registry records for all properties in a given town.
    :param town: The town to update records for.
    :param max_days: Length of time in days before the data is updated.
    :return: None
    """
    land_data = land.LandData()
    land_data.town = town
    cur.execute("""SELECT MAX(date) FROM (SELECT * FROM data_log WHERE postcode_district = :postcode
            AND data_table = 'land_reg')""", {"town": town})
    max_land_reg = cur.fetchall()
    print(str(max_land_reg))
    if max_land_reg[0] == (None,):
        print("No data exists for this postcode district. Getting Land Registry Data...")
        land_data.get_full_price_paid()
    else:
        land_age = datetime.datetime.today() - datetime.datetime.fromisoformat(max_land_reg[0][0])
        if land_age >= max_days:
            print("Land Registry data is more than 1 week old. Updating records...")
            land_data.get_full_price_paid()
            print("Land Registry data updated.")


# Create a merged table in the CDA database
def create_merged_table(cur):
    cur.execute("""DROP TABLE IF EXISTS recent_land;
    CREATE TABLE recent_land as 
    select *, max(transaction_date) as most_recent_transaction from land_reg group by paon, postcode""")

    cur.execute("""DROP TABLE IF EXISTS merged;
    CREATE TABLE merged AS select * from recent_land
             inner join epc 
             on recent_land.postcode = epc.postcode 
             and recent_land.PAON like '%' || epc.address1|| '%'""")

    # # *** Engineer additional features ***
    # # Calculate time since the original transaction
    # self.merged_table['transaction_date'] = pd.to_datetime(self.merged_table['transaction_date'])
    # self.merged_table['transaction_year'] = self.merged_table['transaction_date'].apply(lambda x: x.year)
    # self.merged_table['Days Since Transaction'] = self.merged_table['transaction_date'].apply(lambda x:
    #                                                                                           -(
    #                                                                                                       x - datetime.datetime.today()).days)
    # # Calculate difference in sale price (per sq meter) from area average in a given year
    # self.merged_table['Cost Per Sq M'] = self.merged_table['total_floor_area'] / self.merged_table['price_paid']
    # grouped_by_year = self.merged_table.groupby('transaction_year').agg({'Cost Per Sq M': 'median'})


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
