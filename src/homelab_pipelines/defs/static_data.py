import polars as pl

from homelab_pipelines.utils.paths import Paths

# This csv does not change often. Including it like this means the dagster UI shows all partitions at once.
bybit_symbols = pl.read_csv(
    Paths.defs_data / "bybit_symbols.csv",
    schema_overrides={"launch_time": pl.Datetime("ns", "UTC")},
)
