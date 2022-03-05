import requests

def PostcodeGeocode(postcodes):
    """" This is to return coordinates for a series of postcodes
    :param postcodes: An array of postcodes to be geocoded
    :return: A zipped array of lat/long coordinates
    """
    lat = []
    long = []
    endpoint = 'http://api.postcodes.io/postcodes/'
    # Send the request
    results = requests.post(endpoint, {"postcodes":postcodes}).json()
    # Parse results
    for i in results['result']:
        lat.append(i['result']['latitude'])
        long.append(i['result']['longitude'])
    return zip(lat, long)




