import pandas as pd

# Identify postcode

postcode = 'CH1'

### Import epc data
epc =  pd.read_csv('epc_'+postcode+'.csv')
### Import places data
places =  pd.read_csv('places'+postcode+'.csv')
