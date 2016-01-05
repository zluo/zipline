from zipline.pipeline.data.dividends import CashDividends
from zipline.pipeline.loaders.base import PipelineLoader
from zipline.pipeline.loaders.dividends import CashDividendsLoader
from .core import (
    SID_FIELD_NAME,
    bind_expression_to_resources,
    ffill_query_in_range,
)


class BlazeCashDividendsLoader(PipelineLoader):
    """A pipeline loader for the ``CashDividends`` dataset that loads
    data from a blaze expression.

    Parameters
    ----------
    expr : Expr
        The expression representing the data to load.
    resources : dict, optional
        Mapping from the atomic terms of ``expr`` to actual data resources.
    odo_kwargs : dict, optional
        Extra keyword arguments to pass to odo when executing the expression.

    Notes
    -----
    The expression should have a tabular dshape of::

       Dim * {
           sid: int64,
           declared_date: datetime64,
           ex_date: ?datetime,
           pay_date: ?datetime,
           record_date: ?datetime,
       }

    where each row of the table is a record representing a single cash
    dividend. It is not required that the `?datetime` fields be optional.
    """
    expected_fields = CashDividendsLoader.expected_fields

    def __init__(self,
                 expr,
                 resources=None,
                 odo_kwargs=None,
                 dataset=CashDividends):
        self._expr = bind_expression_to_resources(
            expr[list(self.expected_fields)],
            resources,
        )
        self._dataset = dataset
        self._odo_kwargs = odo_kwargs

    def load_adjusted_array(self, columns, dates, assets, mask):
        raw = ffill_query_in_range(
            self._expr,
            dates[0],
            dates[-1],
            self._odo_kwargs,
            ts_field='declared_date',
        )
        sids = raw.loc[:, SID_FIELD_NAME]
        raw.drop(
            sids[~sids.isin(assets)].index,
            inplace=True
        )
        return CashDividendsLoader(
            raw,
            self._dataset,
        ).load_adjusted_array(
            columns,
            dates,
            assets,
            mask,
        )
