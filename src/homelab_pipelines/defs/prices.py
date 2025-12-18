import datetime as dt
import logging

import dagster as dg
import polars as pl

from homelab_pipelines.resources.bybit import BybitApiV5Resource, GetMarkPriceKlineArgs
from homelab_pipelines.utils.paths import Paths

logger = logging.getLogger(__file__)

bybit_symbols = (
    pl.read_csv(Paths.defs_data / "symbols.csv")
    .get_column("bybit_symbol")
    .sort()
    .to_list()
)
raw_bybit_prices_15min_weekly_partition = dg.MultiPartitionsDefinition(
    {
        "symbol": dg.StaticPartitionsDefinition(bybit_symbols),
        "week": dg.WeeklyPartitionsDefinition(start_date="2025-10-01"),
    }
)


@dg.asset(
    description="Stock prices from Bybit for every 15 minutes partitioned per week.",
    io_manager_key="polars_parquet_io_manager",
    partitions_def=raw_bybit_prices_15min_weekly_partition,
    retry_policy=dg.RetryPolicy(
        max_retries=3,
        delay=30,
        backoff=dg.Backoff.EXPONENTIAL,
        jitter=dg.Jitter.PLUS_MINUS,
    ),
)
def raw_bybit_prices_15min_weekly(
    context: dg.AssetExecutionContext, bybit_api: BybitApiV5Resource
) -> pl.DataFrame:
    symbol, _ = context.partition_key.split("|")
    start = context.partition_time_window.start
    end = context.partition_time_window.end

    context.log.info(
        f"Run id {context.run_id} with partition_key({context.partition_key}), partition_time_window(start={start}, end={end})"
    )

    args = GetMarkPriceKlineArgs(
        symbol=symbol,
        interval="15",
        limit=7 * 24 * 4,  # Number of observations in one week
        start=start,
        end=end,
    )
    result = bybit_api.get_mark_price_kline(args).with_columns(
        symbol=pl.lit(symbol), inserted_at=dt.datetime.now()
    )

    return result
