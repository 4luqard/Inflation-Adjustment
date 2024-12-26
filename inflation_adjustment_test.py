from inflation_adjustment import adjust_series

import pytest
import polars as pl
from polars import col
from polars.testing import assert_frame_equal


import warnings


def ignore_warn(*args, **kwargs):
    """Suppress all warnings."""
    pass


warnings.warn = ignore_warn


@pytest.fixture
def adjusted_series():
    return pl.DataFrame(
        {
            "date": pl.Series("date", ["2005-01-01", "2005-02-01", "2005-03-01"]).cast(
                pl.Date
            ),
            "series": [190.7, 191.8, 193.3],
            "year": [2005, 2005, 2005],
            "month": [1, 2, 3],
            "year_rank": [0, 0, 0],
            "cpi": [190.7, 191.8, 193.3],
            "infadjusted_series": [100.0, 100.0, 100.0],
        },
    )


@pytest.fixture
def original_series():
    return pl.DataFrame(
        {
            "date": ["2005-01-01", "2005-02-01", "2005-03-01"],
            "series": [190.7, 191.8, 193.3],
        },
        schema={"date": pl.Date, "series": pl.Float32},
    )


def test_inflation_adjustment(original_series, adjusted_series):
    returned_series = adjust_series(original_series, "series")
    assert_frame_equal(
        returned_series, adjusted_series, check_column_order=False, check_dtypes=False
    )


if __name__ == "__main__":
    pytest.main([__file__])
