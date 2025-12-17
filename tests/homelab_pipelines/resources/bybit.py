import datetime as dt
from unittest.mock import Mock, patch

import pytest

from homelab_pipelines.resources.bybit import BybitApiV5Resource, GetMarkPriceKlineArgs


@pytest.fixture()
def resource():
    yield BybitApiV5Resource(base_url="https://api-testnet.bybit.com")


class TestBybitApiV5Resource:
    def test_get_mark_price_kline(self, resource: BybitApiV5Resource):
        mock_response = Mock()
        mock_response.json.return_value = {"id": 1, "name": "Alice"}
        mock_response.raise_for_status.return_value = None

        result = resource.get_mark_price_kline(
            GetMarkPriceKlineArgs(
                symbol="BTCUDST",
                interval="5",
                start=dt.datetime(2025, 1, 1, 0, 15),
                end=dt.datetime(2025, 1, 1, 0, 15),
                category="linear",
                limit=200,
            )
        )
