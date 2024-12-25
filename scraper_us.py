import requests
import json
import polars as pl

import warnings
def ignore_warn(*args, **kwargs):
    """Suppress all warnings."""
    pass
warnings.warn = ignore_warn

def scrape_cpi_data_us(series_id: str, date_range: dict) -> pl.DataFrame:
    url = 'https://api.bls.gov/publicAPI/v1/timeseries/data/'
    
    # Request payload
    payload = {
        "seriesid": [series_id],
        "startyear": str(date_range['start']),
        "endyear": str(date_range['end']),
    }
    
    response = requests.post(url, json=payload)
    data = response.json()
    
    # Check for errors in the response
    if data['status'] != 'REQUEST_SUCCEEDED':
        raise ValueError(f"Request failed with status: {data['status']}")
    
    values = []
    for i in range(len(data['Results']['series'][0]['data'])):
        values.append(float(data['Results']['series'][0]['data'][i]['value']))
        
    dataframe = pl.DataFrame({
        'CPI': values[::-1]
    })
    
    if len(dataframe) != (date_range['end'] - date_range['start'] + 1) * 12:
        raise ValueError("Dataframe length does not match the expected length") 
    
    return dataframe
