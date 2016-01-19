"""
Tests for the reference loader for EarningsCalendar.
"""
from unittest import TestCase

import blaze as bz
from blaze.compute.core import swap_resources_into_scope
from contextlib2 import ExitStack
from nose_parameterized import parameterized
import pandas as pd
import numpy as np
from pandas.util.testing import assert_series_equal
from six import iteritems

from zipline.pipeline import Pipeline
from zipline.pipeline.data import BuybackAuth
from zipline.pipeline.engine import SimplePipelineEngine
from zipline.pipeline.factors.events import (
    BusinessDaysSincePreviousBuybackAuth,
)
from zipline.pipeline.loaders.buyback_auth import BuybackAuthorizationsLoader
from zipline.pipeline.loaders.blaze import (
    ANNOUNCEMENT_FIELD_NAME,
    BlazeBuybackAuthorizationsLoader,
    SID_FIELD_NAME,
    TS_FIELD_NAME,
)
from zipline.utils.numpy_utils import make_datetime64D, np_NaT
from zipline.utils.test_utils import (
    make_simple_equity_info,
    tmp_asset_finder,
    gen_calendars,
    to_dataframe,
    num_days_in_range,
)


class BuybackAuthLoaderTestCase(TestCase):
    """
    Tests for loading the earnings announcement data.
    """
    loader_type = BuybackAuthorizationsLoader

    @classmethod
    def setUpClass(cls):
        cls._cleanup_stack = stack = ExitStack()
        cls.sids = A, B, C, D, E = range(5)
        equity_info = make_simple_equity_info(
            cls.sids,
            start_date=pd.Timestamp('2013-01-01', tz='UTC'),
            end_date=pd.Timestamp('2015-01-01', tz='UTC'),
        )
        cls.finder = stack.enter_context(
            tmp_asset_finder(equities=equity_info),
        )

        cls.buyback_authorizations = {
            # K1--K2--A1--A2--SC1--SC2--V1--V2.
            A: to_dataframe(
                ['2014-01-05', '2014-01-10'],
                {"earning_dates": ['2014-01-15', '2014-01-20']},
                {"share_counts": [1, 15, 20, 13],
                 "values": [10, 20, 15, 25]}
            ),
            # K1--K2--E2--E1.
            B: to_dataframe(
                ['2014-01-05', '2014-01-10'],
                {"earning_dates": ['2014-01-20', '2014-01-15']},
                {"share_counts": [7, 13, 12, 2],
                 "values": [10, 22, 19, 20]}
            ),
            # K1--E1--K2--E2.
            C: to_dataframe(
                ['2014-01-05', '2014-01-15'],
                {"earning_dates": ['2014-01-10', '2014-01-20']},
                {"share_counts": [3, 1, 2, 1],
                 "values": [4, 7, 3, 1]}
            ),
            # K1 == K2.
            D: to_dataframe(
                ['2014-01-05'] * 2,
                {"earning_dates": ['2014-01-10', '2014-01-15']},
                {"share_counts": [6, 23, 52, 21],
                 "values": [1, 2, 9, 3]}
            ),
            E: pd.Series(
                data=[],
                index=pd.DatetimeIndex([]),
                dtype='datetime64[ns]'
            ),
        }

    @classmethod
    def tearDownClass(cls):
        cls._cleanup_stack.close()

    def loader_args(self, dates):
        """Construct the base buyback authorizations object to pass to the
        loader.

        Parameters
        ----------
        dates : pd.DatetimeIndex
            The dates we can serve.

        Returns
        -------
        args : tuple[any]
            The arguments to forward to the loader positionally.
        """
        return dates, self.buyback_authorizations

    def setup(self, dates):
        """
        Make a PipelineEngine and expectation functions for the given dates
        calendar.

        This exists to make it easy to test our various cases with critical
        dates missing from the calendar.
        """
        A, B, C, D, E = self.sids

        def num_days_between(start_date, end_date):
            return num_days_in_range(dates, start_date, end_date)

        def zip_with_dates(dts):
            return pd.Series(pd.to_datetime(dts), index=dates)

        _expected_previous_value = pd.DataFrame({
            A: zip_with_dates(
                ['NaT'] * num_days_between(None, '2014-01-04') +
                ['2014-01-15'] * num_days_between('2014-01-05', '2014-01-15') +
                ['2014-01-20'] * num_days_between('2014-01-16', '2014-01-20') +
                ['NaT'] * num_days_between('2014-01-21', None)
            ),
            B: zip_with_dates(
                ['NaT'] * num_days_between(None, '2014-01-04') +
                ['2014-01-20'] * num_days_between('2014-01-05', '2014-01-09') +
                ['2014-01-15'] * num_days_between('2014-01-10', '2014-01-15') +
                ['2014-01-20'] * num_days_between('2014-01-16', '2014-01-20') +
                ['NaT'] * num_days_between('2014-01-21', None)
            ),
            C: zip_with_dates(
                ['NaT'] * num_days_between(None, '2014-01-04') +
                ['2014-01-10'] * num_days_between('2014-01-05', '2014-01-10') +
                ['NaT'] * num_days_between('2014-01-11', '2014-01-14') +
                ['2014-01-20'] * num_days_between('2014-01-15', '2014-01-20') +
                ['NaT'] * num_days_between('2014-01-21', None)
            ),
            D: zip_with_dates(
                ['NaT'] * num_days_between(None, '2014-01-04') +
                ['2014-01-10'] * num_days_between('2014-01-05', '2014-01-10') +
                ['2014-01-15'] * num_days_between('2014-01-11', '2014-01-15') +
                ['NaT'] * num_days_between('2014-01-16', None)
            ),
            E: zip_with_dates(['NaT'] * len(dates)),
        }, index=dates)

        _expected_previous_buyback_share_count = pd.DataFrame({
            A: zip_with_dates(
                ['NaT'] * num_days_between(None, '2014-01-14') +
                ['2014-01-15'] * num_days_between('2014-01-15', '2014-01-19') +
                ['2014-01-20'] * num_days_between('2014-01-20', None)
            ),
            B: zip_with_dates(
                ['NaT'] * num_days_between(None, '2014-01-14') +
                ['2014-01-15'] * num_days_between('2014-01-15', '2014-01-19') +
                ['2014-01-20'] * num_days_between('2014-01-20', None)
            ),
            C: zip_with_dates(
                ['NaT'] * num_days_between(None, '2014-01-09') +
                ['2014-01-10'] * num_days_between('2014-01-10', '2014-01-19') +
                ['2014-01-20'] * num_days_between('2014-01-20', None)
            ),
            D: zip_with_dates(
                ['NaT'] * num_days_between(None, '2014-01-09') +
                ['2014-01-10'] * num_days_between('2014-01-10', '2014-01-14') +
                ['2014-01-15'] * num_days_between('2014-01-15', None)
            ),
            E: zip_with_dates(['NaT'] * len(dates)),
        }, index=dates)

        _expected_previous_buyback_announcement = pd.DataFrame({
            A: zip_with_dates(
                ['NaT'] * num_days_between(None, '2014-01-14') +
                ['2014-01-15'] * num_days_between('2014-01-15', '2014-01-19') +
                ['2014-01-20'] * num_days_between('2014-01-20', None)
            ),
            B: zip_with_dates(
                ['NaT'] * num_days_between(None, '2014-01-14') +
                ['2014-01-15'] * num_days_between('2014-01-15', '2014-01-19') +
                ['2014-01-20'] * num_days_between('2014-01-20', None)
            ),
            C: zip_with_dates(
                ['NaT'] * num_days_between(None, '2014-01-09') +
                ['2014-01-10'] * num_days_between('2014-01-10', '2014-01-19') +
                ['2014-01-20'] * num_days_between('2014-01-20', None)
            ),
            D: zip_with_dates(
                ['NaT'] * num_days_between(None, '2014-01-09') +
                ['2014-01-10'] * num_days_between('2014-01-10', '2014-01-14') +
                ['2014-01-15'] * num_days_between('2014-01-15', None)
            ),
            E: zip_with_dates(['NaT'] * len(dates)),
        }, index=dates)

        _expected_previous_busday_offsets = self._compute_busday_offsets(
            _expected_previous_buyback_announcement
        )

        def expected_previous_value(sid):
            """
            Return the expected next announcement dates for ``sid``.
            """
            return _expected_previous_value[sid]

        def expected_previous_buyback_share_count(sid):
            """
            Return the expected number of days to the next announcement for
            ``sid``.
            """
            return _expected_previous_buyback_share_count[sid]

        def expected_previous_buyback_announcement(sid):
            """
            Return the expected previous announcement dates for ``sid``.
            """
            return _expected_previous_buyback_announcement[sid]

        def expected_previous_busday_offset(sid):
            """
            Return the expected number of days to the next announcement for
            ``sid``.
            """
            return _expected_previous_busday_offsets[sid]

        loader = self.loader_type(*self.loader_args(dates))
        engine = SimplePipelineEngine(lambda _: loader, dates, self.finder)
        return (
            engine,
            expected_previous_value,
            expected_previous_buyback_share_count,
            expected_previous_buyback_announcement,
            expected_previous_busday_offset,
        )

    @staticmethod
    def _compute_busday_offsets(announcement_dates):
        """
        Compute expected business day offsets from a DataFrame of announcement
        dates.
        """
        # Column-vector of dates on which factor `compute` will be called.
        raw_call_dates = announcement_dates.index.values.astype(
            'datetime64[D]'
        )[:, None]

        # 2D array of dates containining expected nexg announcement.
        raw_announce_dates = (
            announcement_dates.values.astype('datetime64[D]')
        )

        # Set NaTs to 0 temporarily because busday_count doesn't support NaT.
        # We fill these entries with NaNs later.
        whereNaT = raw_announce_dates == np_NaT
        raw_announce_dates[whereNaT] = make_datetime64D(0)

        # The abs call here makes it so that we can use this function to
        # compute offsets for both next and previous earnings (previous
        # earnings offsets come back negative).
        expected = abs(np.busday_count(
            raw_call_dates,
            raw_announce_dates
        ).astype(float))

        expected[whereNaT] = np.nan
        return pd.DataFrame(
            data=expected,
            columns=announcement_dates.columns,
            index=announcement_dates.index,
        )

    @parameterized.expand(gen_calendars(
        '2014-01-01',
        '2014-01-31',
        critical_dates=pd.to_datetime([
            '2014-01-05',
            '2014-01-10',
            '2014-01-15',
            '2014-01-20',
        ]),
    ))
    def test_compute_buyback_auth(self, dates):

        (
            engine,
            expected_previous_value,
            expected_previous_buyback_share_count,
            expected_previous_buyback_announcement,
            expected_previous_busday_offset,
        ) = self.setup(dates)

        pipe = Pipeline(
            columns={
                'previous_value': BuybackAuth.previous_buyback_value.latest,
                'previous_buyback_share_count': BuybackAuth.previous_buyback_share_count.latest,
                'previous_buyback_announcement': BuybackAuth.previous_buyback_announcement.latest,
                'days_since_prev': BusinessDaysSincePreviousBuybackAuth(),
            }
        )

        result = engine.run_pipeline(
            pipe,
            start_date=dates[0],
            end_date=dates[-1],
        )

        computed_previous_value = result['previous_value']
        computed_previous_buyback_share_count = result['previous_buyback_share_count']
        computed_previous_buyback_announcement = result['previous_buyback_announcement']
        computed_previous_busday_offset = result['days_since_prev']

        # TODO: NaTs in next/prev should correspond to NaNs in offsets.

        for sid in self.sids:

            assert_series_equal(
                computed_previous_value.xs(sid, level=1),
                expected_previous_value(sid),
                sid,
            )

            assert_series_equal(
                computed_previous_buyback_share_count.xs(sid, level=1),
                expected_previous_buyback_share_count(sid),
                sid,
            )

            assert_series_equal(
                computed_previous_buyback_announcement.xs(sid, level=1),
                expected_previous_buyback_announcement(sid),
                sid,
            )

            assert_series_equal(
                computed_previous_busday_offset.xs(sid, level=1),
                expected_previous_busday_offset(sid),
                sid,
            )


# class BlazeEarningsCalendarLoaderTestCase(EarningsCalendarLoaderTestCase):
#     loader_type = BlazeEarningsCalendarLoader
#
#     def loader_args(self, dates):
#         _, mapping = super(
#             BlazeEarningsCalendarLoaderTestCase,
#             self,
#         ).loader_args(dates)
#         return (bz.Data(pd.concat(
#             pd.DataFrame({
#                 ANNOUNCEMENT_FIELD_NAME: earning_dates,
#                 TS_FIELD_NAME: earning_dates.index,
#                 SID_FIELD_NAME: sid,
#             })
#             for sid, earning_dates in iteritems(mapping)
#         ).reset_index(drop=True)),)


# class BlazeEarningsCalendarLoaderNotInteractiveTestCase(
#         BlazeEarningsCalendarLoaderTestCase):
#     """Test case for passing a non-interactive symbol and a dict of resources.
#     """
#     def loader_args(self, dates):
#         (bound_expr,) = super(
#             BlazeEarningsCalendarLoaderNotInteractiveTestCase,
#             self,
#         ).loader_args(dates)
#         return swap_resources_into_scope(bound_expr, {})


class BuybackAuthLoaderInferTimestampTestCase(TestCase):
    def test_infer_timestamp(self):
        dtx = pd.date_range('2014-01-01', '2014-01-10')
        announcement_dates = {
            0: dtx,
            1: pd.Series(dtx, dtx),
        }
        loader = BuybackAuthorizationsLoader(
            dtx,
            announcement_dates,
            infer_timestamps=True,
        )
        self.assertEqual(
            loader.events_by_sid.keys(),
            announcement_dates.keys(),
        )
        assert_series_equal(
            loader.events_by_sid[0],
            pd.Series(index=[dtx[0]] * 10, data=dtx),
        )
        assert_series_equal(
            loader.events_by_sid[1],
            announcement_dates[1],
        )
