import requests

def PostcodeGeocode(postcodes):
    """" This is to return coordinates for a series of postcodes.
    Each request can only handle 100 postcodes, so this will paginate through until complete.
    :param postcodes: An array of postcodes to be geocoded
    :return: A zipped array of lat/long coordinates
    """
    lat = []
    long = []
    endpoint = 'http://api.postcodes.io/postcodes/'
    # Send the request
    if len(postcodes)>100:
        start = 0
        end = 100
        batch = postcodes[start:end]
        while start < len(postcodes):
            results = requests.post(endpoint, {"postcodes": postcodes}).json()
            # Parse results
            for i in results['result']:
                lat.append(i['result']['latitude'])
                long.append(i['result']['longitude'])
            start += 100
            end += 100
    else:
        results = requests.post(endpoint, {"postcodes":postcodes}).json()
        # Parse results
        for i in results['result']:
            lat.append(i['result']['latitude'])
            long.append(i['result']['longitude'])
        return zip(lat, long)




