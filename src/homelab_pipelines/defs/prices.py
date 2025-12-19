import datetime as dt
from typing import Dict

import dagster as dg
import polars as pl

from homelab_pipelines.resources.bybit import BybitApiV5Resource, GetKlineArgs
from homelab_pipelines.settings import ModelSettings
from homelab_pipelines.utils.paths import Paths

bybit_symbols = (
    pl.read_csv(Paths.defs_data / "symbols.csv")
    .get_column("bybit_symbol")
    .sort()
    .to_list()
)

bybit_symbols_partition = dg.StaticPartitionsDefinition(bybit_symbols)
bybit_week_partitions = dg.WeeklyPartitionsDefinition(start_date="2024-01-01")
raw_bybit_prices_15min_weekly_partition = dg.MultiPartitionsDefinition(
    {
        "symbol": bybit_symbols_partition,
        "week": bybit_week_partitions,
    }
)


@dg.asset(
    description="Stock prices from Bybit for every 15 minutes partitioned per week.",
    kinds={"python", "polars"},
    io_manager_key="polars_parquet_io_manager",
    partitions_def=raw_bybit_prices_15min_weekly_partition,
    retry_policy=dg.RetryPolicy(
        max_retries=3,
        # Delay for 15 minutes, see https://bybit-exchange.github.io/docs/v5/rate-limit
        delay=15 * 60,
        backoff=dg.Backoff.EXPONENTIAL,
        jitter=dg.Jitter.PLUS_MINUS,
    ),
)
def raw_bybit_prices_15min_weekly(
    context: dg.AssetExecutionContext, bybit_api: BybitApiV5Resource
) -> pl.DataFrame:
    symbol, _ = context.partition_key.split("|")
    partition_start = context.partition_time_window.start
    partition_end = context.partition_time_window.end

    context.log.info(f"Starting run_id {context.run_id} for symbol {symbol}")

    # The get_kline time interval is inclusive by start_time, thus, we only want
    args = GetKlineArgs(
        symbol=symbol,
        interval="15",
        limit=7 * 24 * 4,  # Number of observations in one week
        start=partition_start,
        end=partition_end - dt.timedelta(minutes=15),
    )
    context.log.info(f"Fetching kline with args {args}")
    result = bybit_api.get_kline(args).with_columns(
        symbol=pl.lit(symbol), ingested_at=dt.datetime.now()
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
        )
    },
)
def stg_bybit_prices_15min(
    context: dg.AssetExecutionContext,
    raw_bybit_prices_15min_weekly: Dict[str, pl.DataFrame],
) -> pl.DataFrame:
    context.log.info(f"Combining {len(raw_bybit_prices_15min_weekly)} dataframes")

    result = pl.concat(raw_bybit_prices_15min_weekly.values())
    context.log.info(f"Created dataframe of shape {result.shape}")

    return result


@dg.asset(
    description="Train dataset for each symbol",
    kinds={"python", "polars"},
    io_manager_key="polars_parquet_io_manager",
    partitions_def=bybit_symbols_partition,
    automation_condition=dg.AutomationCondition.eager(),
    ins={"stg_bybit_prices_15min": dg.AssetIn(key="stg_bybit_prices_15min")},
)
def stg_prices_15min_model_train(
    context: dg.AssetExecutionContext,
    stg_bybit_prices_15min: pl.DataFrame,
) -> pl.DataFrame:
    model_settings = ModelSettings()
    context.log.info(f"Processing dataframe of shape {stg_bybit_prices_15min.shape}")

    result = stg_bybit_prices_15min.filter(
        pl.col("dt_utc").cast(pl.Datetime())
        >= dt.datetime.now() - dt.timedelta(days=model_settings.n_days_history_train)
    )

    min_dt: dt.datetime = result.get_column("dt_utc").min()  # type: ignore
    max_dt: dt.datetime = result.get_column("dt_utc").max()  # type: ignore

    n_days_history = (max_dt - min_dt).days

    if n_days_history < model_settings.n_days_history_train_min:
        raise ValueError(
            f"Not enough data to create training dataset, received {n_days_history} while {model_settings.n_days_history_train_min} are required"
        )

    context.log.info(f"Dataframe shape after processing {result.shape}")
    return result
