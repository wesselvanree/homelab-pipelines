import datetime as dt

import pytz


class DateTime:
    @staticmethod
    def last_monday(d: dt.date) -> dt.date:
        return d - dt.timedelta(days=d.weekday())

    @staticmethod
    def start_of_week_utc(value: dt.datetime) -> dt.datetime:
        return value.combine(
            DateTime.last_monday(value.date()),
            dt.time(0, 0),
            tzinfo=pytz.timezone("UTC"),
        )
