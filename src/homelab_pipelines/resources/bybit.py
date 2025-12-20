import datetime as dt
import logging
from typing import Annotated, Dict, Literal, Optional
from urllib import request
from urllib.parse import urlencode

import dagster as dg
import polars as pl
import requests
from pydantic import BaseModel, Field

from homelab_pipelines.utils.errors import EndTimeBeforeStartTimeError
from homelab_pipelines.utils.flatten_dict import flatten_dict
from homelab_pipelines.utils.polars import rename_columns_to_snake_case

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


class GetInstrumentInfoArgs(BaseModel):
    symbol: str
    category: Literal["spot", "linear", "inverse", "option"] = "linear"
    limit: Annotated[int, Field(500, ge=1, le=1000)] = 500


class BybitApiV5Resource(dg.ConfigurableResource):
    base_url: str

    def get_kline(self, args: GetKlineArgs) -> pl.DataFrame:
        if args.end <= args.start:
            raise EndTimeBeforeStartTimeError(
                f"Start date ({args.start}) should be before end date ({args.end})"
            )

        args_dict = args.model_dump()
        args_dict["start"] = self._dt_to_bybit_timestamp(args_dict["start"])
        args_dict["end"] = self._dt_to_bybit_timestamp(args_dict["end"])

        data = self._get_endpoint("/v5/market/kline", args_dict)

        result = (
            pl.DataFrame(
                data["result"]["list"],
                orient="row",
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

    def get_instrument_info(self, args: GetInstrumentInfoArgs) -> pl.DataFrame:
        args_dict = args.model_dump()

        data = self._get_endpoint("/v5/market/instruments-info", args_dict)

        result = pl.DataFrame([flatten_dict(item) for item in data["result"]["list"]])
        result = rename_columns_to_snake_case(result)
        result = result.select(
            pl.col("symbol"),
            pl.col("status"),
            pl.col("base_coin"),
            pl.col("quote_coin"),
            pl.from_epoch(pl.col("launch_time").cast(pl.Int64), time_unit="ms").cast(
                pl.Datetime("ns", "UTC")
            ),
            pl.col("price_filter__min_price").cast(pl.Float32),
            pl.col("price_filter__max_price").cast(pl.Float32),
            pl.col("price_filter__tick_size").cast(pl.Float32),
            pl.col("lot_size_filter__min_order_qty").cast(pl.Float32),
            pl.col("lot_size_filter__max_order_qty").cast(pl.Float32),
            pl.col("lot_size_filter__qty_step").cast(pl.Float32),
            pl.col("lot_size_filter__max_mkt_order_qty").cast(pl.Float32),
        )

        return result

    def _get_endpoint(
        self, endpoint: str, query_params: Optional[Dict[str, str | int | float]]
    ):
        url = f"{self.base_url}{endpoint}"

        if query_params is not None:
            url += f"?{urlencode(query_params)}"

        response = requests.get(url)
        response.raise_for_status()

        data = response.json()
        return data

    def _dt_to_bybit_timestamp(self, value: dt.datetime) -> int:
        return int(value.timestamp()) * 1000
