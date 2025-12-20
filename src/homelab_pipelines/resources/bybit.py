import datetime as dt
import logging
from typing import Annotated, Literal
from urllib.parse import urlencode

import dagster as dg
import polars as pl
import requests
from pydantic import BaseModel, Field

from homelab_pipelines.utils.errors import EndTimeBeforeStartTimeError

logger = logging.getLogger(__name__)


class GetKlineArgs(BaseModel):
    symbol: str
    interval: Literal[
        "1", "3", "5", "15", "30", "60", "120", "240", "360", "720", "D", "M", "W"
    ]
    start: dt.datetime
    """Data with a start_time larger or equal to this are included."""
    end: dt.datetime
    """Data with a start_time smaller or equal to this are included."""
    category: Literal["spot", "linear", "inverse"] = "linear"
    limit: Annotated[int, Field(200, ge=1, le=1000)] = 200


class BybitApiV5Resource(dg.ConfigurableResource):
    base_url: str

    def get_kline(self, args: GetKlineArgs) -> pl.DataFrame:
        if args.end <= args.start:
            raise EndTimeBeforeStartTimeError(
                f"Start date ({args.start}) should be before end date ({args.end})"
            )

        url = f"{self.base_url}/v5/market/kline"
        args_dict = args.model_dump()
        args_dict["start"] = self._dt_to_bybit_timestamp(args_dict["start"])
        args_dict["end"] = self._dt_to_bybit_timestamp(args_dict["end"])
        url += f"?{urlencode(args_dict)}"

        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        result = (
            pl.DataFrame(
                data["result"]["list"],
                schema={
                    "start_time_ms": pl.Int64(),
                    "open": pl.Float32(),
                    "high": pl.Float32(),
                    "low": pl.Float32(),
                    "close": pl.Float32(),
                    "volume": pl.Float32(),
                    "turnover": pl.Float32(),
                },
            )
            .with_columns(
                pl.from_epoch("start_time_ms", time_unit="ms")
                .cast(pl.Datetime("us", "UTC"))
                .alias("start_time_utc")
            )
            # Reorder columns
            .select(
                "start_time_utc", "open", "high", "low", "close", "volume", "turnover"
            )
        )

        return result

    def _dt_to_bybit_timestamp(self, value: dt.datetime) -> int:
        return int(value.timestamp()) * 1000
