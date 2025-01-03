from inflation_us import scrape_cpi_data_us

import polars as pl
from polars import col
import numpy as np

import warnings


def ignore_warn(*args, **kwargs):
    """Suppress all warnings."""
    pass


warnings.warn = ignore_warn


def adjust_series(data: pl.DataFrame, series: str, decimal: int = 1) -> pl.DataFrame:

    data = data.with_columns(
        [
            (col("date").dt.year()).alias("year"),
            (col("date").dt.month()).alias("month"),
        ]
    )

    data = data.with_columns((col("year") - col("year").min()).alias("year_rank"))

    date_range = {
        "start": data["year"].min(),
        "end": data["year"].max(),
    }

    cpi_data = (
        scrape_cpi_data_us(date_range)["CPI"].to_pandas().values.round(decimals=1)
    )

    data = data.with_columns(
        [
            pl.Series("cpi", cpi_data[data["year_rank"] * 12 + (data["month"] - 1)]),
            (
                (col(series) * 100)
                / cpi_data[data["year_rank"] * 12 + (data["month"] - 1)].round(decimal)
            ).alias(f"infadjusted_{series}"),
        ]
    )

    return data
