import pandas as pd
from sklearn import neighbors
import matplotlib.pyplot as plt

# Identify postcode

postcode = 'CH1'

# Import epc data
epc = pd.read_csv('epc_' + postcode + '.csv')

# Import places data
places = pd.read_csv('places_' + postcode + '.csv')

epc.head()

places.head()

df = pd.merge(places, epc, how='left', left_on='UPRN', right_on='uprn')

df['construction-age-band'].value_counts(dropna=False)


# Need to clean the 'construction-age-band' field

def set_age_cat(x):
    if x in ['England and Wales: 2007 onwards']:
        return 'England and Wales: 2007-2011'
    elif x in ['2020', '2021']:
        return 'England and Wales: 2012 onwards'
    elif x in ['NO DATA!', 'INVALID!']:
        return None
    else:
        return x


df['construction-age-band'] = df['construction-age-band'].apply(lambda x: set_age_cat(x))

# Remove null values

full_df = df.copy()  # Keep a copy of the full dataset

df.dropna(axis=0, subset=['construction-age-band'], inplace=True)

missing_age_df = full_df[~full_df['uprn'].isin(df['uprn'])]

# KNN classifier

# Create a training/testing set

training_df = df.sample(n=int(len(df) / 2))

test_df = df[~df['uprn'].isin(training_df['uprn'])]

# Need to determine optimal value for K
# Create a list to log error rates of
k_results = []

for k in range(1, 20):

    clf = neighbors.KNeighborsClassifier(k, weights="distance")

    clf.fit(training_df[['X_COORDINATE', 'Y_COORDINATE']], training_df['construction-age-band'])

    k_results.append(clf.score(test_df[['X_COORDINATE', 'Y_COORDINATE']], test_df['construction-age-band']))

# Identify optimal value for k
plt.plot(range(1, 20), k_results)

optimal_k = k_results.index(max(k_results)) + 1

clf = neighbors.KNeighborsClassifier(optimal_k, weights="distance")

clf.fit(training_df[['X_COORDINATE', 'Y_COORDINATE']], training_df['construction-age-band'])

# Create a new column for the estimated age bands and one for the confidence of those estimates
missing_age_df['construction-age-band'] = pd.Series(clf.predict(missing_age_df[['X_COORDINATE', 'Y_COORDINATE']]))
missing_age_df['construction-age-band-estimated'] = True
df['construction-age-band-estimated'] = False
missing_age_df['construction-age-band-estimated-confidence'] = pd.Series(list(map(max,clf.predict_proba(missing_age_df[['X_COORDINATE', 'Y_COORDINATE']]))))

output = pd.concat([df, missing_age_df], axis=0)
output['construction-age-band'].value_counts(dropna = False)


# Still 4940 missing values for the missing age df -- Need to find these