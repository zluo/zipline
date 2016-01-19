"""
Reference implementation for EarningsCalendar loaders.
"""

from events import EventsLoader
from .frame import DataFrameLoader
from .utils import previous_value
from ..data.buyback_auth import BuybackAuthorizations
from zipline.utils.memoize import lazyval


class BuybackAuthorizationsLoader(EventsLoader):
    """
    Reference loader for
    :class:`zipline.pipeline.data.earnings.BuybackAuthorizations`.

    Does not currently support adjustments to the dates of known buyback
    authorizations.

    """

    def __init__(self, all_dates, events_by_sid, infer_timestamps=False,
                dataset=BuybackAuthorizations):
        super(BuybackAuthorizationsLoader, self).__init__(all_dates,
                                                          events_by_sid,
                                                          infer_timestamps,
                                                          dataset=dataset)


    def get_loader(self, column):
        """dispatch to the loader for ``column``.
        """

        if column is self.dataset.previous_buyback_value:
            return self.previous_buyback_value_loader
        elif column is self.dataset.previous_buyback_share_count:
            return self.previous_buyback_share_count_loader
        elif column is self.dataset.previous_buyback_value_announcement:
            return self.previous_buyback_value_announcement_loader
        elif column is self.dataset.previous_buyback_share_count_announcement:
            return self.previous_buyback_share_count_announcement_loader
        else:
            raise ValueError("Don't know how to load column '%s'." % column)


    @lazyval
    def previous_buyback_value_announcement_loader(self):
        return self._previous_announcement_loader(
            self.dataset.previous_buyback_value_announcement)

    @lazyval
    def previous_buyback_share_count_announcement_loader(self):
        return self._previous_announcement_loader(
            self.dataset.previous_buyback_share_count_announcement)

    @lazyval
    def previous_buyback_share_count_loader(self):
        return DataFrameLoader(
            self.dataset.previous_buyback_share_count,
            previous_value(
                self.all_dates,
                self.events_by_sid,
            ),
            adjustments=None,
        )

    @lazyval
    def previous_buyback_value_loader(self):
        return DataFrameLoader(
            self.previous_buyback_value_loader,
            previous_value(
                self.all_dates,
                self.events_by_sid,
            ),
            adjustments=None,
        )

