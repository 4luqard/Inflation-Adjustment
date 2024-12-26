import requests
import json
import polars as pl

import warnings


def ignore_warn(*args, **kwargs):
    """Suppress all warnings."""
    pass


warnings.warn = ignore_warn


def scrape_cpi_data_us(
    date_range: dict, series_id: str = "CUUR0000SA0"
) -> pl.DataFrame:
    url = "https://api.bls.gov/publicAPI/v1/timeseries/data/"

    if date_range["end"] == str or date_range["start"] == str:
        raise TypeError("unsupported operand type(s) for -: 'str' and 'str'")

    wanted_year_amount = date_range["end"] - date_range["start"] + 1

    #! With the public api at most ten years worth of data can be requested at ones.
    ten_year_requests_amount = wanted_year_amount // 10
    remaining_years = wanted_year_amount % 10

    if ten_year_requests_amount == 0 or (
        ten_year_requests_amount == 1 and remaining_years == 0
    ):
        # Request payload
        payload = [
            {
                "seriesid": [series_id],
                "startyear": str(date_range["start"]),
                "endyear": str(date_range["end"]),
            }
        ]
    else:
        # Request payload
        payload = []
        for i in range(ten_year_requests_amount):

            start = date_range["start"] + i * 10
            end = start + 9 + i * 10
            payload.append(
                {
                    "seriesid": [series_id],
                    "startyear": str(start),
                    "endyear": str(end),
                }
            )
        payload.append(
            {
                "seriesid": [series_id],
                "startyear": str(date_range["end"] - remaining_years + 1),
                "endyear": str(date_range["end"]),
            }
        )

    data = []
    for k in range(len(payload)):
        response = requests.post(url, json=payload[k])

        # Check for errors in the response
        if response.json()["status"] != "REQUEST_SUCCEEDED":
            raise ValueError(f"Request failed with status: {response.json()['status']}")

        data.append(response.json())

    values = []
    for j in range(len(data)):
        value = []
        for i in range(len(data[j]["Results"]["series"][0]["data"])):
            value.append(float(data[j]["Results"]["series"][0]["data"][i]["value"]))
        values += value[::-1]

    dataframe = pl.DataFrame({"CPI": values})

    if len(dataframe) != (date_range["end"] - date_range["start"] + 1) * 12:
        raise ValueError("Dataframe length does not match the expected length")

    return dataframe


# print(
#     scrape_cpi_data_us("CUUR0000SA0", {"start": 1986, "end": 2000})["CPI"]
#     .to_pandas()
#     .values
# )
