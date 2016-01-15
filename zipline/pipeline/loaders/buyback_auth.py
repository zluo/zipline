"""
Reference implementation for EarningsCalendar loaders.
"""
from functools import partial

from events import EventsLoader
from .utils import previous_date_frame
from ..data.buyback_auth import BuybackAuthorizations


class BuybackAuthorizationsLoader(EventsLoader):
    """
    Reference loader for
    :class:`zipline.pipeline.data.earnings.BuybackAuthorizations`.

    Does not currently support adjustments to the dates of known buyback
    authorizations.

    """

    def __init__(self, dataset=None):
        super(BuybackAuthorizationsLoader, self).__init__(dataset=BuybackAuthorizations)

    def get_loader(self, column):
        """dispatch to the loader for ``column``.
        """

        if column is self.dataset.previous_buyback_value:
            return partial(self.date_frame_loader,
                           self.dataset.previous_buyback_value,
                           previous_date_frame)
        elif column is self.dataset.previous_buyback_share_count:
            return partial(self.date_frame_loader,
                           self.dataset.previous_buyback_share_count,
                           previous_date_frame)
        elif column is self.dataset.previous_buyback_value_announcement:
            return partial(self.date_frame_loader,
                           self.dataset.previous_buyback_value_announcement,
                           previous_date_frame)
        elif column is self.dataset.previous_buyback_share_count_announcement:
            return partial(self.date_frame_loader,
                           self.dataset.previous_buyback_share_count_announcement,
                           previous_date_frame)
        else:
            raise ValueError("Don't know how to load column '%s'." % column)



