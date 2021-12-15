import pandas as pd
import sklearn

# Identify postcode

postcode = 'CH1'

### Import epc data
epc =  pd.read_csv('epc_'+postcode+'.csv')

### Import places data
places =  pd.read_csv('places_'+postcode+'.csv')

epc.head()

places.head()

df = pd.merge(places, epc, how = 'left', left_on = 'UPRN', right_on = 'uprn')

df['construction-age-band'].value_counts(dropna = False)

### Need to clean the 'construction-age-band' field

# Remove null values
df.dropna(axis= 0,subset=['construction-age-band'], inplace= True)

# Identify other variables to remove
# For larger or messier datasets, might be easier to list what to keep...
variables_to_remove = ['NO DATA!','INVALID!']

df = df[~df['construction-age-band'].isin(variables_to_remove)]

#### KNN classifier



## Need to determine optimal value for K

# Create a list to log error rates of
k_results = []
