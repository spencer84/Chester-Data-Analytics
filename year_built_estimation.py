import pandas as pd

# Identify postcode

postcode = 'CH1'

### Import epc data
epc =  pd.read_csv('epc_'+postcode+'.csv')

### Import places data
places =  pd.read_csv('places_'+postcode+'.csv')

epc.head()

places.head()

df = pd.merge(places, epc, how = 'left', left_on = 'UPRN', right_on = 'uprn')

df['construction-age-band'].head()
