import dagster as dg

from homelab_pipelines.resources.bybit import BybitApiV5Resource
from homelab_pipelines.settings import BybitSettings
from homelab_pipelines.utils.paths import Paths


@dg.definitions
def defs():
    return dg.load_from_defs_folder(path_within_project=Paths.package_root)


@dg.definitions
def resources():
    return dg.Definitions(
        resources={"bybit_v5": BybitApiV5Resource(base_url=BybitSettings().base_url)}
    )
