import datetime as dt

from homelab_pipelines.utils.datetime import Datetime


class TestDatetime:
    def test_local_tz(self):
        assert isinstance(Datetime.local_tz, str)

    def test_now_utc(self):
        assert isinstance(Datetime.now_utc(), dt.datetime)
        assert Datetime.now_utc().isoformat().endswith("+00:00")

    def test_last_monday(self):
        assert Datetime.last_monday(dt.date(2025, 12, 15)) == dt.date(2025, 12, 15)
        assert Datetime.last_monday(dt.date(2025, 12, 19)) == dt.date(2025, 12, 15)
        assert Datetime.last_monday(dt.date(2026, 1, 1)) == dt.date(2025, 12, 29)

    def test_start_of_week_utc(self):
        assert Datetime.start_of_week_utc(
            dt.datetime.fromisoformat("2025-12-19T12:00:00+00:00")
        ) == dt.datetime.fromisoformat("2025-12-15T00:00:00+00:00")

        assert Datetime.start_of_week_utc(
            dt.datetime.fromisoformat("2025-12-19T12:00:00+01:00")
        ) == dt.datetime.fromisoformat("2025-12-15T00:00:00+00:00")

        assert Datetime.start_of_week_utc(
            dt.datetime.fromisoformat("2025-12-15T00:00:00+00:00")
        ) == dt.datetime.fromisoformat("2025-12-15T00:00:00+00:00")
