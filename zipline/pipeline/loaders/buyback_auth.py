"""
Reference implementation for EarningsCalendar loaders.
"""
from itertools import repeat

import pandas as pd
from six import iteritems


from ..data.buyback_auth import BuybackAuthorizations
from events import EventsLoader
from .frame import DataFrameLoader
from .utils import previous_value, previous_date_frame
from zipline.utils.memoize import lazyval


BUYBACK_ANNOUNCEMENT_FIELD_NAME = 'buyback_dates'
SHARE_COUNT_FIELD_NAME = 'share_counts'
VALUE_FIELD_NAME = 'values'

class BuybackAuthorizationsLoader(EventsLoader):
    """
    Reference loader for
    :class:`zipline.pipeline.data.earnings.BuybackAuthorizations`.

    Does not currently support adjustments to the dates of known buyback
    authorizations.

    events_by_sid: dict[sid -> pd.DataFrame(knowledge date,
    event date, value)]

    """

    def __init__(self,
                 all_dates,
                 events_by_sid,
                 infer_timestamps=False,
                 dataset=BuybackAuthorizations):
        self.all_dates = all_dates
        self.events_by_sid = (
            events_by_sid.copy()
        )
        dates = self.all_dates.values
        # TODO: what to do about this logic given that now I have a DataFrame
        #  coming in, and I don't think df columns can be DatetimeIndex?
        for k, v in iteritems(events_by_sid):
            if isinstance(v[BUYBACK_ANNOUNCEMENT_FIELD_NAME], pd.DatetimeIndex):
                if not infer_timestamps:
                    raise ValueError(
                        "Got DatetimeIndex of announcement dates for sid %d.\n"
                        "Pass `infer_timestamps=True` to use the first date in"
                        " `all_dates` as implicit timestamp."
                    )
                # If we are passed a DatetimeIndex, we always have
                # knowledge of the announcements.
                events_by_sid[k]['previous_buyback_value_announcement'] = pd.Series(
                    v['previous_buyback_value_announcement']
                    ['event_date'],
                    index=repeat(dates[0],
                                 len(v['previous_buyback_value_announcement'])),
                )
                events_by_sid[k]['previous_buyback_share_count_announcement'] = pd.Series(
                    v['previous_buyback_share_count_announcement']
                    ['event_date'],
                    index=repeat(dates[0],
                                 len(v['previous_buyback_share_count_announcement'])),
                )
        self.dataset = dataset


    def get_loader(self, column):
        """dispatch to the loader for ``column``.
        """
        if column is self.dataset.previous_buyback_value:
            return self.previous_buyback_value_loader
        elif column is self.dataset.previous_buyback_share_count:
            return self.previous_buyback_share_count_loader
        elif column is self.dataset.previous_buyback_announcement:
            return self.previous_buyback_announcement_loader
        else:
            raise ValueError("Don't know how to load column '%s'." % column)


    @lazyval
    def previous_buyback_announcement_loader(self):
        return DataFrameLoader(
            self.dataset.previous_buyback_announcement,
            previous_date_frame(
                self.all_dates,
                {sid: frame[BUYBACK_ANNOUNCEMENT_FIELD_NAME] for sid, frame in
                 self.events_by_sid.iteritems()},
            ),
            adjustments=None,
        )

    @lazyval
    def previous_buyback_share_count_loader(self):
        return DataFrameLoader(
            self.dataset.previous_buyback_share_count,
            previous_value(
                self.all_dates,
                self.events_by_sid,
                BUYBACK_ANNOUNCEMENT_FIELD_NAME,
                SHARE_COUNT_FIELD_NAME
            ),
            adjustments=None,
        )

    @lazyval
    def previous_buyback_value_loader(self):
        return DataFrameLoader(
            self.dataset.previous_buyback_value,
            previous_value(
                self.all_dates,
                self.events_by_sid,
                BUYBACK_ANNOUNCEMENT_FIELD_NAME,
                VALUE_FIELD_NAME
            ),
            adjustments=None,
        )

