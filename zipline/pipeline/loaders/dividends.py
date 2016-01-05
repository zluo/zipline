import pandas as pd
from toolz import valmap, merge

from .base import PipelineLoader
from .utils import next_date_frame, previous_date_frame
from zipline.pipeline.loaders.synthetic import DataFrameLoader
from zipline.pipeline.data.dividends import CashDividends


class CashDividendsLoader(PipelineLoader):
    """
    Reference loader for
    :class:`zipline.pipeline.data.dividends.CashDividends`.

    Does not currently support adjustments.

    Parameters
    ----------
    data : pd.DataFrame
        The raw table of data. This should have the following fields:

        * ``declared_date``
        * ``ex_date``
        * ``pay_date``
        * ``record_date``
        * ``sid``

        The order of the columns does not matter. Each row should represent a
        single cash dividend.
    dataset : DataSet, optional
        The dataset to bind this loader to.

    See Also
    --------
    :class:`zipline.pipline.loaders.blaze.dividends.BlazeCashDividendsLoader`
    """

    expected_fields = frozenset({
        'declared_date',
        'ex_date',
        'pay_date',
        'record_date',
        'sid',
    })

    def __init__(self, data, dataset=CashDividends):
        self._data = data
        self._dataset = dataset
        if not self.expected_fields <= set(data.columns):
            raise ValueError(
                'mismatched columns: %r != %r' % (
                    set(data.columns),
                    self._expected_fields
                ),
            )

    @staticmethod
    def _load_date(column, dates, assets, raw, gb, mask):
        name = column.name
        if name.startswith('next_'):
            prefix = 'next_'
            to_frame = next_date_frame
        elif name.startswith('previous_'):
            prefix = 'previous_'
            to_frame = previous_date_frame
        else:
            raise AssertionError(
                "column name should start with 'next_' or 'previous_',"
                ' got %r' % name,
            )

        def mkseries(idx, raw_loc=raw.loc):
            vs = raw_loc[
                idx, ['declared_date', name[len(prefix):]],
            ].values
            return pd.Series(
                index=pd.DatetimeIndex(vs[:, 0]),
                data=vs[:, 1],
            )

        return DataFrameLoader(
            column,
            to_frame(
                dates,
                valmap(mkseries, gb.groups),
            ),
            adjustments=None,
        ).load_adjusted_array([column], dates, assets, mask)

    def load_adjusted_array(self, columns, dates, assets, mask):
        if set(columns).isdisjoint(self._dataset.columns):
            raise ValueError(
                'columns could not be loaded: %r' %
                set(columns).symmetric_difference(self._dataset.columns),
            )

        raw = self._data
        gb = raw[raw['sid'].isin(assets)].groupby('sid')

        return merge(*(
            self._load_date(column, dates, assets, raw, gb, mask)
            for column in columns
        ))
