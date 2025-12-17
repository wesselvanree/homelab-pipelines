import datetime as dt

from homelab_pipelines.resources.bybit import BybitApiV5Resource, GetMarkPriceKlineArgs
from homelab_pipelines.settings import BybitSettings


def main() -> None:
    """Do the work."""

    bybit_settings = BybitSettings()
    resource = BybitApiV5Resource(base_url=bybit_settings.base_url)

    args = GetMarkPriceKlineArgs(
        symbol="BTCUSDT",
        interval="5",
        start=dt.datetime.fromisoformat("2025-01-01T00:15+01:00"),
        end=dt.datetime.fromisoformat("2025-01-01T00:30+01:00"),
        category="linear",
        limit=200,
    )
    result = resource.get_mark_price_kline(args)

    print(result)


if __name__ == "__main__":
    main()
