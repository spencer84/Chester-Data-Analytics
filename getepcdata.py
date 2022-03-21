import requests
import json
import base64
import pandas as pd
import sqlite3
import datetime

# Retrieve OS Maps API Key
# Use .gitignore to hide JSON file
with open('API Key.json') as f:
    api_key = json.loads(f.read())
key = api_key['epc_key']
username = api_key['epc_username']
url = 'https://epc.opendatacommunities.org/api/v1/domestic/search'  # URL for the Places API

# Need to encode the username and key then strip off the extra bits
encoded_api_key = str(base64.b64encode(bytes(username + ':' + key, 'utf-8')))[1:].replace('\'', "")


def get_postcode_data(key, postcode):
    """" Returns a pandas dataframe containing all results for the given postcode.
    This can be written to a CSV file for later processing.
    :param key: Encoded secret key for the OS API
    :param postcode: Any UK Postcode or the first part (district portion)
    :return: Pandas Dataframe of EPC results
    """
    # Can only paginate through the first 10,000 results of any query. Need to use energy bands (or other parameter)
    # to break up the query.
    energy_bands = ['a', 'b', 'c', 'd', 'e', 'f', 'g']
    results = []
    for band in energy_bands:
        results_not_reached = True
        total_count = 0
        while results_not_reached:
            response = requests.get(url, params={'postcode': postcode, 'size': 5000, 'from': total_count,
                                                 'energy-band': band},
                                    headers={'Authorization': 'Basic %s' % key, "Accept": 'application/json'})
            results += response.json()['rows']
            count = len(response.json()['rows'])
            if count < 5000:  # The idea here is that if the response
                results_not_reached = False
            total_count += count
    results = pd.DataFrame(data=results, columns=response.json()['column-names'])
    return results


# Select postcode to search by
postcode = 'CH1'

# Call the function
results = get_postcode_data(encoded_api_key, postcode)

# Write to DB
con = sqlite3.connect('cda.db')
cur = con.cursor()

# Initialise the table if not already done
# Let's start with a smaller one with a few key values
# Query date is reflective of the date the data is requested
cur.execute('''CREATE TABLE IF NOT EXISTS epc 
            (address text, address1 text ,uprn text,postcode text, current_energy_rating text, total_floor_area real,
            lodgement_datetime text, query_date text
            )''')
# Create a string variable for the current datetime
curr_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Iterate through the results dataframe, adding each value
for index, row in results.iterrows():
    cur.execute("INSERT INTO epc VALUES(?,?,?,?,?,?,?,?)", (row.address, row.address1, row.uprn, row['postcode'],
                                                            row['current-energy-rating'], row['total-floor-area'],
                                                            row['lodgement-datetime'], curr_time))

# Test for edge cases
cur.execute("SELECT * FROM epc WHERE address = '12, Hillside Road, Blacon'")
rows = cur.fetchall()
print(rows)
# How to handle multiple EPC records on same property? Take most recent
cur.execute("""DELETE FROM epc where (address, lodgement_datetime) NOT IN (SELECT 
  epc.address, epc.lodgement_datetime
FROM
  (SELECT
     address, MAX(lodgement_datetime) AS most_recent_epc
   FROM
     epc
   GROUP BY
     address) AS latest_record
INNER JOIN
  epc
ON
  epc.address = latest_record.address AND
  epc.lodgement_datetime = latest_record.most_recent_epc)""")
# cur.execute("DROP TABLE epc")
# Drop duplicate records based on the most recent query_date
# Is the address the best variable to filter by? In the test case above, there are two entries for the same address...
cur.execute("""DELETE FROM epc WHERE query_date < (SELECT max(query_date) FROM epc) AND address IN
            (SELECT address FROM epc GROUP BY address HAVING COUNT(*) >1)""")

rows = cur.fetchall()
print(rows)
# Commit the changes to save all updates

cur.execute("COMMIT;")

# Close the connection

cur.close()

# cur.execute('''CREATE TABLE IF NOT EXISTS epc
#                (lmk_key text, address1 text, address2 text, address3 text,
#                 building_reference_number text, current_energy_rating text,	potential_energy_rating text,
#                 current_energy_efficiency real,	potential_energy_efficiency real,
#                 property_type text,	built_form text, inspection_date text,	local_authority	constituency text,
#                 county text, lodgement_date text, transaction_type text, environment_impact_current real,
#                 environment_impact_potential real,	energy_consumption_current real, energy_consumption_potential real,
#                 co2_emissions_current real,	co2_emiss_curr_per_floor_area real,	co2_emissions_potential real,
#                 lighting_cost_current real,	lighting_cost_potential real, heating_cost_current real,
#                 heating_cost_potential real, hot_water_cost_current	real, hot_water_cost_potential real,
#                 total_floor_area real,	energy_tariff text,	mains_gas_flag text, floor_level text,
#                 flat_top_storey text, flat_storey_count int, main_heating_controls text,
#                 multi_glaze_proportion real, glazed_type text,	glazed_area	extension_count int,
#                 number_habitable_rooms int,	number_heated_rooms int,low_energy_lighting	int,
#                 number_open_fireplaces int,	hotwater_description text,	hot_water_energy_eff text,
#                 hot_water_env_eff text,	floor_description text,	floor_energy_eff text,	floor_env_eff text,
#                 windows_description text,	windows_energy_eff text, windows_env_eff text, walls_description text,
#                 walls_energy_eff text,	walls_env_eff text,	secondheat_description text, sheating_energy_eff text,
#                 sheating_env_eff text,	roof_description text,	roof_energy_eff text,	roof_env_eff text,
#                 mainheat_description text,	mainheat_energy_eff text, mainheat_env_eff text,
#                 mainheatcont_description text,	mainheatc_energy_eff text,	mainheatc_env_eff text,
#                 lighting_description text,	lighting_energy_eff	text, lighting_env_eff text, main_fuel text,
#                 wind_turbine_count int,	heat_loss_corridor text, unheated_corridor_length real,	floor_height real,
#                 photo_supply text, solar_water_heating_flag text, mechanical_ventilation text,
#                 address text,	local_authority_label text,	constituency_label text, posttown text,
#                 construction_age_band text,	lodgement_datetime text, tenure	 text, fixed_lighting_outlets_count int,
#                 low_energy_fixed_light_count int, uprn text, uprn_source text)''')

# Insert a row of data


# Save (commit) the changes
con.commit()

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
con.close()
# results.to_csv('epc_' +postcode + '.csv')
