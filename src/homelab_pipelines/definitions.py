import dagster as dg
from dagster_polars import PolarsParquetIOManager

from homelab_pipelines.resources.bybit import BybitApiV5Resource
from homelab_pipelines.settings import BybitSettings
from homelab_pipelines.utils.paths import Paths


@dg.definitions
def defs():
    bybit_settings = BybitSettings()  # type: ignore

    return dg.Definitions.merge(
        dg.Definitions(
            resources={
                "bybit_api": BybitApiV5Resource(base_url=bybit_settings.base_url),
                "polars_parquet_io_manager": PolarsParquetIOManager(
                    base_dir=str(Paths.repo_root / "output")
                ),
            }
        ),
        dg.load_from_defs_folder(path_within_project=Paths.package_root),
    )
