import openmeteo_requests
import requests_cache
from retry_requests import retry
import pandas as pd
import numpy as np
from datetime import date


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


def create_historic_dataset(params: dict, file_path: str, session=create_session()):
    url = "https://archive-api.open-meteo.com/v1/archive"
    if not "start_date" in params.keys():
        params["start_date"] = "2021-01-01"
    if not "end_date" in params.keys():
        params["end_date"] = date.today().strftime("%Y-%m-%d")
    request = session.weather_api(url, params=params)[0]
    responses = []
    if "hourly" in params.keys():
        responses += [(request.Hourly(), "hourly")]
    if "minutely_15" in params.keys():
        responses += [(request.Minutely15(), "minutely_15")]
    
    for response, label in responses:
        data = {
            "date": pd.date_range(
                start=pd.to_datetime(response.Time(), unit = "s"),
                end=pd.to_datetime(response.TimeEnd(), unit = "s"),
                freq=pd.Timedelta(seconds=response.Interval()),
                inclusive="left"
                )
            }
        for i in range(response.VariablesLength()):  # Iterating through all the variables in params["hourly"]
            data[params[label][i]] = response.Variables(i).ValuesAsNumpy()

        historic_df = pd.DataFrame(data=data)
        historic_df.dropna().to_csv(file_path + "historic_" + label + ".csv")
        return historic_df


if __name__ == "__main__":
    create_historic_dataset(
        params={
            "latitude": 48.81452162820077,
            "longitude": 2.3948078406657114,
            "hourly": ["temperature_2m", 
                "relative_humidity_2m", 
                "wind_speed_10m",
                "wind_direction_10m",
                "wind_speed_80m",
                "wind_direction_80m",
                "apparent_temperature",
                "pressure_msl",
                "cloud_cover",
                "precipitation", 
                "weather_code"
                ],
        },
        file_path="./data/",
        session=create_session()
    )