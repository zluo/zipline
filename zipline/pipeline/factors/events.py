"""
Factors describing information about event data (e.g. earnings
announcements, acquisitions, dividends, etc.).
"""
from numpy import newaxis
from zipline.pipeline.data.buyback_auth import BuybackAuthorizations
from zipline.pipeline.data.earnings import EarningsCalendar
from zipline.utils.numpy_utils import (
    np_NaT,
    busday_count_mask_NaT,
    datetime64D_dtype,
    float64_dtype,
)

from .factor import Factor


class BusinessDaysSincePreviousEvents(Factor):
    """
    Abstract class for business days since a previous event.
    """
    window_length = 0
    dtype = float64_dtype

    def _compute(self, arrays, dates, assets, mask):

        # Coerce from [ns] to [D] for numpy busday_count.
        announce_dates = arrays[0].astype(datetime64D_dtype)

        # Set masked values to NaT.
        announce_dates[~mask] = np_NaT

        # Convert row labels into a column vector for broadcasted comparison.
        reference_dates = dates.values.astype(datetime64D_dtype)[:, newaxis]
        return busday_count_mask_NaT(announce_dates, reference_dates)


class BusinessDaysUntilNextEvents(Factor):
    """
    Abstract class for business days since a next event.
    """
    window_length = 0
    dtype = float64_dtype

    def _compute(self, arrays, dates, assets, mask):

        # Coerce from [ns] to [D] for numpy busday_count.
        announce_dates = arrays[0].astype(datetime64D_dtype)

        # Set masked values to NaT.
        announce_dates[~mask] = np_NaT

        # Convert row labels into a column vector for broadcasted comparison.
        reference_dates = dates.values.astype(datetime64D_dtype)[:, newaxis]
        return busday_count_mask_NaT(reference_dates, announce_dates)



class BusinessDaysUntilNextEarnings(BusinessDaysUntilNextEvents):
    """
    Factor returning the number of **business days** (not trading days!) until
    the next known earnings date for each asset.

    This doesn't use trading days because the trading calendar includes
    information that may not have been available to the algorithm at the time
    when `compute` is called.

    For example, the NYSE closings September 11th 2001, would not have been
    known to the algorithm on September 10th.

    Assets that announced or will announce earnings today will produce a value
    of 0.0.  Assets that will announce earnings on the next upcoming business
    day will produce a value of 1.0.

    Assets for which `EarningsCalendar.next_announcement` is `NaT` will produce
    a value of `NaN`.


    See Also
    --------
    zipline.pipeline.factors.BusinessDaysSincePreviousEarnings
    """
    inputs = [EarningsCalendar.next_announcement]


class BusinessDaysSincePreviousEarnings(BusinessDaysSincePreviousEvents):
    """
    Factor returning the number of **business days** (not trading days!) since
    the most recent earnings date for each asset.

    This doesn't use trading days for symmetry with
    BusinessDaysUntilNextEarnings.

    Assets which announced or will announce earnings today will produce a value
    of 0.0.  Assets that announced earnings on the previous business day will
    produce a value of 1.0.

    Assets for which `EarningsCalendar.previous_announcement` is `NaT` will
    produce a value of `NaN`.

    See Also
    --------
    zipline.pipeline.factors.BusinessDaysUntilNextEarnings
    """
    inputs = [EarningsCalendar.previous_announcement]


class BusinessDaysSincePreviousBuybackAuth(BusinessDaysSincePreviousEvents):
    """
    Factor returning the number of **business days** (not trading days!) since
    the most recent buyback authorization for each asset.

    This doesn't use trading days for symmetry with
    BusinessDaysUntilNextEarnings.

    Assets which announced or will announce buyback authorizations today will
    produce a value of 0.0.
    Assets that announced a buyback authorization on the previous business day
    will produce a value of 1.0.

    Assets for which `BuybackAuthorization.previous_announcement` is `NaT` will
    produce a value of `NaN`.

    See Also
    --------
    zipline.pipeline.factors.BusinessDaysUntilNextEarnings
    """
    inputs = [BuybackAuthorizations.previous_buyback_announcement]

