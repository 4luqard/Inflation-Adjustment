import pytest
import numpy as np
import polars as pl
from scraper_us import scrape_cpi_data_us

import warnings
def ignore_warn(*args, **kwargs):
    pass
warnings.warn = ignore_warn

@pytest.fixture
def data_ranges():
    return {
        'data_range': {'start': 1986, 'end': 1986},
        'empty_data_range': {'start': '', 'end': ''},
        'invalid_data_range': {'start': 'invalid-date', 'end': 'invalid-date'},
        'future_data_range': {'start': 2050, 'end': 2050}
    }

@pytest.fixture
def cpi_data_us():
    return pl.DataFrame({
        'CPI': [109.6, 109.3, 108.8, 108.6, 108.9, 109.5, 109.5, 109.7, 110.2, 110.3,  110.4, 110.5]
    })

def test_scrape_cpi_data_us(data_ranges, cpi_data_us):
    result = scrape_cpi_data_us('CUUR0000SA0', data_ranges['data_range'])
    assert result['CPI'].equals(cpi_data_us['CPI'])

def test_scrape_cpi_data_us_empty_range(data_ranges):
    with pytest.raises(TypeError, match=r"unsupported operand type\(s\) for -: 'str' and 'str'"):
        scrape_cpi_data_us('CUUR0000SA0', data_ranges['empty_data_range'])

def test_scrape_cpi_data_us_invalid_range(data_ranges):
    with pytest.raises(ValueError, match="Request failed with status: REQUEST_FAILED_INVALID_PARAMETERS"):
        scrape_cpi_data_us('CUUR0000SA0', data_ranges['invalid_data_range'])

def test_scrape_cpi_data_us_future_range(data_ranges):
    with pytest.raises(ValueError, match="Request failed with status: REQUEST_FAILED_INVALID_PARAMETERS"):
        scrape_cpi_data_us('CUUR0000SA0', data_ranges['future_data_range'])

if __name__ == "__main__":
    pytest.main()