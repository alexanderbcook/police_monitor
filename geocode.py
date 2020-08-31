import geocoder

def geocode(unformattedAddress):

    address = formatAddress(unformattedAddress)
    mapBias = setProximity(unformattedAddress)

    g = geocoder.mapbox(address, proximity=mapBias, maxRows=1, key='pk.eyJ1IjoiYWJjb29rIiwiYSI6ImNqcnp5N3N2ZzFkeWs0NG80bnVtNmFxaDEifQ.zPkUuVuuVNGg-3DkQcBflQ')
    return g.json
def formatAddress(unformattedAddress):

    formattedAddress = unformattedAddress.replace(' NE ', ' NORTHEAST ')
    formattedAddress.replace(' NW ', ' NORTHWEST ')
    formattedAddress.replace(' SE ',' SOUTHEAST ')
    formattedAddress.replace(' SW ',' SOUTHWEST ')
    formattedAddress.replace(' S ',' SOUTH ')
    formattedAddress.replace(' N ',' NORTH ')
    formattedAddress.replace(' / ', ' & ')
    formattedAddress +', PORTLAND, OREGON'

    return formattedAddress

def setProximity(unformattedAddress):
    if ' NE ' in unformattedAddress:
         mapCenter = [45.5676,-122.6179]
    elif ' SE ' in unformattedAddress:
         mapCenter = [45.4914,-122.5930]
    elif ' NW ' in unformattedAddress:
         mapCenter = [45.5586,-122.7609]
    elif ' SW ' in unformattedAddress:
         mapCenter = [45.4914,-122.5930]
    elif ' N ' in unformattedAddress:
         mapCenter = [45.6075,-122.7236]
    elif ' S ' in unformattedAddress:
         mapCenter = [45.4888,-122.6747]
    elif ' W ' in unformattedAddress:
         mapCenter = [45.4475,-122.7221]
    elif ' E ' in unformattedAddress:
         mapCenter = [45.5154,-122.6604]
    else:
         mapCenter = [45.5051,-122.6750]

    return mapCenter
