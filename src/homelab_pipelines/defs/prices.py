import datetime as dt
from os import name
from typing import Dict, Optional

import dagster as dg
import polars as pl

from homelab_pipelines.defs.static_data import bybit_symbols
from homelab_pipelines.resources.bybit import BybitApiV5Resource, GetKlineArgs
from homelab_pipelines.settings import ModelSettings
from homelab_pipelines.utils.datetime import Datetime

bybit_symbols_partition = dg.StaticPartitionsDefinition(
    bybit_symbols.get_column("symbol").to_list()
)
weekly_partition = dg.WeeklyPartitionsDefinition(
    start_date="2023-01-01",
    day_offset=1,  # Start partition on Monday instead of Sunday
)
bybit_symbol_weekly_partition = dg.MultiPartitionsDefinition(
    {
        "symbol": bybit_symbols_partition,
        "week": weekly_partition,
    }
)

bybit_retry_policy = dg.RetryPolicy(
    max_retries=3,
    # Delay for 15 minutes, see https://bybit-exchange.github.io/docs/v5/rate-limit
    delay=15 * 60,
    backoff=dg.Backoff.EXPONENTIAL,
    jitter=dg.Jitter.PLUS_MINUS,
)


@dg.asset(
    description="Stock prices from Bybit for every 15 minutes partitioned per week.",
    kinds={"python", "polars"},
    io_manager_key="polars_parquet_io_manager",
    partitions_def=bybit_symbol_weekly_partition,
    retry_policy=bybit_retry_policy,
)
def raw_bybit_prices_15min_weekly(
    context: dg.AssetExecutionContext, bybit_api: BybitApiV5Resource
) -> Optional[pl.DataFrame]:
    symbol, _ = context.partition_key.split("|")
    partition_start = context.partition_time_window.start
    partition_end = context.partition_time_window.end

    context.log.info(
        f"Starting run_id {context.run_id} for symbol {symbol} in range {partition_start}, {partition_end}"
    )

    bybit_symbol = bybit_symbols.filter(pl.col("symbol") == symbol).row(0, named=True)

    if partition_start < bybit_symbol["launch_time"]:
        return None

    # The get_kline time interval is inclusive by start_time.
    # Thus, we subtract 15 minutes to only fetch closed ones.
    args = GetKlineArgs(
        symbol=symbol,
        interval="15",
        limit=7 * 24 * 4,  # Number of observations in one week
        start=partition_start,
        end=partition_end - dt.timedelta(minutes=15),
    )
    context.log.info(f"Fetching kline with args {args}")
    result = bybit_api.get_kline(args).with_columns(
        symbol=pl.lit(symbol),
        ingested_at=pl.lit(Datetime.now_utc()).dt.convert_time_zone(Datetime.local_tz),
    )

    return result


@dg.asset(
    description="Stock prices from Bybit for every 15 minutes starting from the first day of this week until the current run time. This asset can be used to add to the weekly partitioned asset to form a dataset that includes everything until the most recent run.",
    kinds={"python", "polars"},
    io_manager_key="polars_parquet_io_manager",
    partitions_def=bybit_symbols_partition,
    retry_policy=bybit_retry_policy,
)
def raw_bybit_prices_15min_recent(
    context: dg.AssetExecutionContext, bybit_api: BybitApiV5Resource
) -> pl.DataFrame:
    symbol = context.partition_key
    context.log.info(f"Starting run_id {context.run_id} for symbol {symbol}")

    # The get_kline time interval is inclusive by start_time.
    # Thus, we subtract 15 minutes to only fetch closed ones.
    args = GetKlineArgs(
        symbol=symbol,
        interval="15",
        limit=7 * 24 * 4,  # Number of observations in one week
        start=Datetime.start_of_week_utc(Datetime.now_utc()),
        end=Datetime.now_utc() - dt.timedelta(minutes=15),
    )
    context.log.info(f"Fetching kline with args {args}")
    result = bybit_api.get_kline(args).with_columns(
        symbol=pl.lit(symbol),
        ingested_at=pl.lit(Datetime.now_utc()).dt.convert_time_zone(Datetime.local_tz),
    )

    return result


@dg.asset(
    description="Complete dataset for a single symbol",
    kinds={"python", "polars"},
    io_manager_key="polars_parquet_io_manager",
    partitions_def=bybit_symbols_partition,
    automation_condition=dg.AutomationCondition.eager(),
    ins={
        "raw_bybit_prices_15min_weekly": dg.AssetIn(
            partition_mapping=dg.MultiToSingleDimensionPartitionMapping("symbol")
        ),
        "raw_bybit_prices_15min_recent": dg.AssetIn(),
    },
)
def stg_bybit_prices_15min(
    context: dg.AssetExecutionContext,
    raw_bybit_prices_15min_weekly: Dict[str, pl.DataFrame],
    raw_bybit_prices_15min_recent: pl.DataFrame,
) -> pl.DataFrame:
    context.log.info(f"Combining {len(raw_bybit_prices_15min_weekly)} dataframes")

    result = pl.concat(raw_bybit_prices_15min_weekly.values())
    context.log.info(f"Concatenated weeks into a dataframe of shape {result.shape}")

    observations_to_append = raw_bybit_prices_15min_recent.filter(
        pl.col("start_time_utc") >= result.get_column("start_time_utc").max()
    )

    if len(observations_to_append) > 0:
        result = pl.concat([result, observations_to_append])
        context.log.info(
            f"Added this week ({len(observations_to_append)} rows), resulting in shape {result.shape}"
        )
    else:
        context.log.info("No observations to append from this week")

    n_combined = len(result)

    # Remove duplicates
    result = (
        result.with_columns(
            pl.col("start_time_utc")
            .rank()
            .over(
                partition_by=["symbol", "start_time_utc"],
                order_by="ingested_at",
                descending=True,
            )
            .alias("rank")
        )
        .filter(pl.col("rank") <= 1)
        .drop("rank")
    )
    context.log.info(f"Removed {n_combined - len(result)} duplicates")

    return result


@dg.asset(
    description="Train dataset for each symbol",
    kinds={"python", "polars"},
    io_manager_key="polars_parquet_io_manager",
    partitions_def=bybit_symbols_partition,
    automation_condition=dg.AutomationCondition.eager(),
    ins={"stg_bybit_prices_15min": dg.AssetIn()},
)
def stg_prices_15min_model_train(
    context: dg.AssetExecutionContext,
    stg_bybit_prices_15min: pl.DataFrame,
) -> pl.DataFrame:
    model_settings = ModelSettings()
    context.log.info(f"Processing dataframe of shape {stg_bybit_prices_15min.shape}")

    result = stg_bybit_prices_15min.filter(
        pl.col("start_time_utc").cast(pl.Datetime())
        >= dt.datetime.now() - dt.timedelta(days=model_settings.n_days_history_train)
    )

    min_dt: dt.datetime = result.get_column("start_time_utc").min()  # type: ignore
    max_dt: dt.datetime = result.get_column("start_time_utc").max()  # type: ignore

    n_days_history = (max_dt - min_dt).days

    if n_days_history < model_settings.n_days_history_train_min:
        raise ValueError(
            f"Not enough data to create training dataset, received {n_days_history} while {model_settings.n_days_history_train_min} are required"
        )

    context.log.info(f"Dataframe shape after processing {result.shape}")
    return result
