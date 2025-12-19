import datetime as dt

from homelab_pipelines.resources.bybit import BybitApiV5Resource, GetKlineArgs
from homelab_pipelines.settings import BybitSettings


def main() -> None:
    """Do the work."""

    bybit_settings = BybitSettings()
    resource = BybitApiV5Resource(base_url=bybit_settings.base_url)

    result = resource.get_kline(
        GetKlineArgs(
            symbol="BTCUSDT",
            interval="15",
            start=dt.datetime.fromisoformat("2025-01-01T00:00+00:00"),
            end=dt.datetime.fromisoformat("2025-01-07T00:00+00:00"),
            category="linear",
            limit=1000,
        )
    )

    print(result)


if __name__ == "__main__":
    main()
