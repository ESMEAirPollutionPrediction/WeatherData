import openmeteo_requests
import requests_cache
from retry_requests import retry
import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim


wmo_codes = pd.DataFrame({"code": [0, 1, 2, 3, 45, 48, 51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 71, 73, 75, 77, 80, 81, 82, 85, 86, 95, 96, 99]})
wmo_codes['description'] = np.select(
    [wmo_codes['code'].isin([0]),
     wmo_codes['code'].isin([1, 2, 3]),
     wmo_codes['code'].isin([45, 48]),
     wmo_codes['code'].isin([51, 53, 55]),
     wmo_codes['code'].isin([56, 57]),
     wmo_codes['code'].isin([61, 63, 65]),
     wmo_codes['code'].isin([66, 67]),
     wmo_codes['code'].isin([71, 73, 75]),
     wmo_codes['code'].isin([77]),
     wmo_codes['code'].isin([80, 81, 82]),
     wmo_codes['code'].isin([85, 86]),
     wmo_codes['code'].isin([95]),
     wmo_codes['code'].isin([96, 99]),
    ],
    ['Clear sky',
     'Mainly clear, partly cloudy, and overcast',
     'Fog and depositing rime fog',
     'Drizzle: Light, moderate, and dense intensity',
     'Freezing Drizzle: Light and dense intensity',
     'Rain: Slight, moderate and heavy intensity',
     'Freezing Rain: Light and heavy intensity',
     'Snow fall: Slight, moderate, and heavy intensity',
     'Snow grains',
     'Rain showers: Slight, moderate, and violent',
     'Snow showers slight and heavy',
     'Thunderstorm: Slight or moderate',
     'Thunderstorm with slight and heavy hail'
    ]
)


def get_wmo_description(code: int | None, all=False):
    if all:
        return wmo_codes
    if code in wmo_codes.index:
        return wmo_codes.at[code, 'description']
    return


def create_session():
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)
    return openmeteo

