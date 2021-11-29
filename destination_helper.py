from geopy.geocoders import Nominatim, GoogleV3
from geopy.distance import great_circle
from django.conf import settings


geo = GoogleV3(api_key=settings.GOOGLE_GEOCODING_API_KEY)


def get_distance_by_address(location, address, limit=5, threshold=1.75):
    locations_list = []
    address = geo.geocode(address, components={"city": "New York", "country": "United States"})
    if not address:
        print(f'User address is not found . Address {address}')
        return locations_list
    lat_long_address = (address.latitude, address.longitude)
    for location in location.filter(address__isnull=False):
        loc = geo.geocode(location.address, components={"city": "New York", "country": "United States"})

        if not loc:
            print(f"Location the address is not found. Address {location.address}")
            continue

        lat_long = (loc.latitude, loc.longitude)
        measuring_distance = great_circle(lat_long_address, lat_long).miles

        if measuring_distance <= threshold:
            locations_list.append((location, measuring_distance))
    locations_list = sorted(locations_list, key=lambda tup: tup[1])[:limit]
    return [item[0] for item in locations_list]

