import dagster as dg
import polars as pl

from homelab_pipelines.defs.prices import bybit_symbol_weekly_partition
from homelab_pipelines.defs.static_data import bybit_symbols

PRICES_ASSET_KEY = "raw_bybit_prices_15min_weekly"
PRICES_MAX_PARTITIONS_PER_RUN = 100


@dg.schedule(
    cron_schedule="*/15 * * * *",
    description="Backfilling all prices at once may introduce issues regarding API limits. This schedule checks missing symbols, and requests only one backfill at each step.",
    target=PRICES_ASSET_KEY,
)
def incremental_backfill__raw_bybit_prices_15min_weekly(
    context: dg.ScheduleEvaluationContext,
):
    context.log.info("Checking for bybit prices to backfill...")

    materialized_paritions = context.instance.get_materialized_partitions(
        asset_key=dg.AssetKey(PRICES_ASSET_KEY),
    )
    all_keys = {str(key) for key in bybit_symbol_weekly_partition.get_partition_keys()}
    missing_keys = all_keys - materialized_paritions

    # We only want to rerun partitions where we expect data.
    # This means data before the launch_time of a symbol is omitted
    missing = (
        pl.DataFrame({"partition_key": list(missing_keys)})
        .with_columns(
            pl.col("partition_key")
            .str.split("|")
            .list.get(0, null_on_oob=True)
            .alias("symbol"),
            pl.col("partition_key")
            .str.split("|")
            .list.get(1, null_on_oob=True)
            .alias("week")
            .cast(pl.Date),
        )
        .filter((pl.col("symbol").is_not_null()) & pl.col("week").is_not_null())
        .join(bybit_symbols.select("symbol", "launch_time"), on="symbol", how="inner")
        .filter(pl.col("launch_time").dt.date() < pl.col("week"))
    )

    context.log.info(f"Found {len(missing)} valid missing partitions.")

    if len(missing) == 0:
        return dg.SkipReason("No missing price data within bounds.")

    missing_partitions = (
        # The most recent data is the most relevant
        missing.sort(["week", "symbol"], descending=True)
        .limit(PRICES_MAX_PARTITIONS_PER_RUN)
        .get_column("partition_key")
        .to_list()
    )
    context.log.info(f"Requesting runs for {len(missing_partitions)} partitions...")

    return [
        dg.RunRequest(
            partition_key=p,
        )
        for p in missing_partitions
    ]
