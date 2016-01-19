"""
Tests BoundColumn attributes and methods.
"""
from contextlib2 import ExitStack
from unittest import TestCase

from pandas import date_range, DataFrame
from pandas.util.testing import assert_frame_equal

from zipline.pipeline import Pipeline
from zipline.pipeline.data.testing import TestingDataSet as TDS
from zipline.utils.test_utils import chrange, temp_pipeline_engine


class LatestTestCase(TestCase):

    @classmethod
    def setUpClass(cls):
        cls._stack = stack = ExitStack()
        cls.calendar = cal = date_range('2014', '2015', freq='D', tz='UTC')
        cls.sids = list(range(5))
        cls.engine = stack.enter_context(
            temp_pipeline_engine(
                cal,
                cls.sids,
                random_seed=100,
                symbols=chrange('A', 'E'),
            ),
        )
        cls.assets = cls.engine._finder.retrieve_all(cls.sids)

    @classmethod
    def tearDownClass(cls):
        cls._stack.close()

    def expected_latest(self, column, slice_):
        loader = self.engine.get_loader(column)
        return DataFrame(
            loader.values(column.dtype, self.calendar, self.sids)[slice_],
            index=self.calendar[slice_],
            columns=self.sids,
        )

    def test_latest(self):
        pipe = Pipeline(
            columns={
                name: getattr(TDS, name + '_col').latest
                # Intentionally not including int and bool because they're not
                # yet supported.
                for name in ('float', 'datetime')
            }
        )

        cal_slice = slice(20, 40)
        dates_to_test = self.calendar[cal_slice]
        result = self.engine.run_pipeline(
            pipe,
            dates_to_test[0],
            dates_to_test[-1],
        )
        float_result = result.float.unstack()
        expected_float_result = self.expected_latest(TDS.float_col, cal_slice)
        assert_frame_equal(float_result, expected_float_result)