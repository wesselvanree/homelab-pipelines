import datetime as dt

from tzlocal import get_localzone_name


class DateTime:
    local_tz = get_localzone_name()

    @staticmethod
    def now_utc() -> dt.datetime:
        return dt.datetime.now(dt.timezone.utc)

    @staticmethod
    def last_monday(d: dt.date) -> dt.date:
        return d - dt.timedelta(days=d.weekday())

    @staticmethod
    def start_of_week_utc(value: dt.datetime) -> dt.datetime:
        return value.combine(
            DateTime.last_monday(value.date()),
            dt.time(0, 0),
            tzinfo=dt.timezone.utc,
        )
