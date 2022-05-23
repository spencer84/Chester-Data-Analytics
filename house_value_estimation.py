import getepcdata as epc
import getpricepaid as land
import sqlite3
import datetime
import pandas as pd
import numpy as np
import requests
from sklearn import neighbors
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.metrics import mean_absolute_percentage_error

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
        cols.append(i[0])  # The first value of this tuple is the column name--retrieve and append to list of cols
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


def find_nearby_postcodes(postcode):
    url = 'https://api.postcodes.io/postcodes/' + postcode + '/nearest'
    results = requests.get(url)
    nearby_postcodes = [results.json()['result'][x]['postcode'] for x in results.json()['result']]
    return nearby_postcodes


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
        self.prop_features = None
        self.model = None
        self.model_perf = None

    def get_input(self):
        """
        Get data from user
        :return: self, updated address attributes
        """
        if not self.postcode:
            postcode = input("Postcode:")
            self.postcode = postcode
        res = self.validate_postcode()
        if res:
            print('Postcode is valid')
        else:
            print('Postcode NOT valid. Try again.')
            self.get_input()
        if not self.number:
            number = input("House/Flat Number:")
        # Postcode doesn't need to be sanitized as it is verified, but other input values do
            self.number = number
        self.postcode_district = get_postcode_district(self.postcode)
        if not self.town:
            town = input("Town:")
            self.town = town

    def validate_postcode(self):
        """
        Check the user-provided postcode and determine if it is a valid UK postcode
        :return: Boolean value, whether postcode is valid or not
        """
        url = 'https://api.postcodes.io/postcodes/' + self.postcode + '/validate'
        request = requests.get(url)
        return request.json()['result']

    def check_postcode_data(self):
        # Connect to db
        print('Checking postcode data...')
        con = sqlite3.connect(self.db)
        cur = con.cursor()
        cur.execute("SELECT * FROM data_log WHERE postcode_district =? ", (self.postcode_district,))
        results = cur.fetchall()
        # Find most recent EPC records
        cur.execute("""SELECT MAX(date) FROM (SELECT * FROM data_log WHERE postcode_district = :postcode  
        AND data_table = 'epc' )""", {"postcode": self.postcode_district})
        max_epc = cur.fetchall()
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
        # Create the LandData Object and assign attributes needed for query
        land_data = land.LandData()
        land_data.town = self.town
        land_data.create_connection('cda.db')
        land_data.create_cursor()
        cur.execute("""SELECT MAX(date) FROM (SELECT * FROM data_log WHERE postcode_district = :postcode
        AND data_table = 'land_reg')""", {"postcode": self.postcode_district})
        max_land_reg = cur.fetchall()
        print(str(max_land_reg))
        if max_land_reg[0] == (None,):
            print("No data exists for this postcode district. Getting Land Registry Data...")
            land_data.get_full_price_paid()
        else:
            land_age = datetime.datetime.today() - datetime.datetime.fromisoformat(max_land_reg[0][0])
            if land_age.days > 30:
                update_land = input("Land Registry Data is more than 30 days old. Update? y/n")
                if update_land == 'y':
                    land_data.get_full_price_paid()
                elif update_land == 'n':
                    pass
                else:
                    print("Input not understood...Data will not be updated.")

    def return_cursor(self):
        con = sqlite3.connect(self.db)
        cur = con.cursor()
        return cur

    def create_merged_table(self):
        """Once data is updated and checked, create new attributes of the property object as Pandas DataFrames
        containing both the relevant data to create the prediction model
        """
        # Are Pandas DataFrames redundant if I just do this in SQL?
        # Create EPC DataFrame
        epc_query = f"SELECT * FROM epc WHERE postcode like '{str(self.postcode_district)}%'"
        self.epc_table = sql_query_to_df(self.return_cursor(), epc_query)
        # Create Land Registry Price Paid DataFrame
        land_reg_query = f"SELECT * FROM land_reg WHERE postcode_district = '{str(self.postcode_district)}'"
        self.land_reg_table = sql_query_to_df(self.return_cursor(), land_reg_query)
        # This merge query joins the tables where the postcode is the same and where the epc address1 field contains
        # the PAON from the land_reg table.
        # Still need to check for edge cases...
        merge_query = """select * from land_reg
         inner join epc 
         on land_reg.postcode = epc.postcode 
         and land_reg.PAON like '%' || epc.address1|| '%'"""
        self.merged_table = sql_query_to_df(self.return_cursor(), merge_query)
        # *** Engineer additional features ***
        # Calculate time since the original transaction
        self.merged_table['transaction_date'] = pd.to_datetime(self.merged_table['transaction_date'])
        self.merged_table['transaction_year'] = self.merged_table['transaction_date'].apply(lambda x: x.year)
        self.merged_table['Days Since Transaction'] = self.merged_table['transaction_date'].apply(lambda x:
                                                                                x-datetime.datetime.today())
        # Calculate difference in sale price (per sq meter) from area average in a given year
        self.merged_table['Cost Per Sq M'] = self.merged_table['total_floor_area']/self.merged_table['price_paid']
        grouped_by_year = self.merged_table.groupby('transaction_year').agg({'Cost Per Sq M':'median'})
        # Need to parse this groupby object to get the area median cost per sq meter in the next step
        #self.merged_table['Area Median £ per Sq M'] = self.merged_table['']

    # What other pre-model processing is needed? Removal of outliers?
    # Remove Null values?

    def create_model(self):
        ### Build Model based on the relationship between price paid and area

        # Training data
        X = np.array(self.merged_table['total_floor_area'])
        X = X.reshape(-1, 1)

        # Target data
        y = np.array(self.merged_table['price_paid'])
        y = y.reshape(-1, 1)

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)

        self.model = LinearRegression()
        self.model.fit(X_train, y_train)
        y_pred = self.model.predict(X_test)
        self.model_perf = {
            "Coefficients": self.model.coef_,
            "Mean squared error": mean_squared_error(y_test, y_pred),
            "Coefficient of determination": r2_score(y_test, y_pred),
            "MAPE": mean_absolute_percentage_error(y_test, y_pred)
        }
        print("Model created")

    def check_features(self):
        """Evaluate whether it is necessary to create synthetic data for the given property. If the property
        features cannot be found, then create and assign them using the methods found in the 'create synthetic features
        method."""
        # Query data for specified property
        print("Merged table cols:",self.merged_table.columns)
        prop_features = self.merged_table[(self.merged_table['postcode'] == self.postcode) & (self.merged_table['PAON'] == self.number)]
        # Check if the results have the needed features for the model
        try:
            prop_features = prop_features[0]  # Not sure how to handle more than one results...
        except KeyError:  # If there aren't any results, run the method to create synthetics
            # If features not found, create synthetic features from results in the postcode
            # and adjacent postcodes. Need to look up adjacent postcodes
            prop_features = self.create_synthetic_features()
        finally:
            self.prop_features = prop_features

    def create_synthetic_features(self):
        """The EPC dataset will not contain all properties. Where an EPC record is not available, a KNN model will be
        used to extrapolate features based on neighbours."""
        # Find the nearest postcodes
        nearby_postcodes = find_nearby_postcodes()
        # Including the original postcode
        nearby_postcodes.append(self.postcode)
        neighbors_df = self.merged_table[['Postcode'].isin(nearby_postcodes)]
        # Choose median value of neighbours
        synth_features = neighbors_df.median()
        # Convert this to a Numpy array
        return synth_features

    # def predict(self):
    #     # Need to first check if an EPC record is available for the given record
    #     cur = self.return_cursor()
    #     cur.execute("SELECT * WHERE", )
    #     self.model.predict(y)


# Identify property for estimate
prop = Property()
prop.postcode = 'CH1 1SD'
prop.postcode_district = 'CH1'
prop.number = '12'
prop.town = 'CHESTER'
prop.get_input()
prop.check_postcode_data()
# cur = prop.return_cursor()
prop.create_merged_table()
prop.check_features()
print(prop.prop_features)
prop.create_model()
#prop.predict()

# If model is created for each postcode, should we create a seperate model class inhereted from a postcode class?
# print(prop.merged_table.head())

# Quickly create a new table to log data sources and track when updated
# test = 'CH2'
# import sqlite3
# con = sqlite3.connect('cda.db')
# cur = con.cursor()
#
# results  = cur.execute("SELECT * FROM data_log WHERE postcode_district =?",(test,))
