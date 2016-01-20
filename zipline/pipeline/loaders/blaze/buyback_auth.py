from datashape import istabular
import pandas as pd
from toolz import valmap

from .core import (
    TS_FIELD_NAME,
    SID_FIELD_NAME,
    bind_expression_to_resources,
    ffill_query_in_range,
)
from zipline.pipeline.data import BuybackAuthorizations
from zipline.pipeline.loaders.base import PipelineLoader
from zipline.pipeline.loaders.buyback_auth import BuybackAuthorizationsLoader
from zipline.pipeline.loaders.utils import (
    check_data_query_args,
    normalize_data_query_bounds,
    normalize_timestamp_to_query_time,
)
from zipline.utils.input_validation import ensure_timezone, optionally
from zipline.utils.preprocess import preprocess


BUYBACK_ANNOUNCEMENT_FIELD_NAME = 'buyback_dates'
SHARE_COUNT_FIELD_NAME = 'share_counts'
VALUE_FIELD_NAME = 'values'


class BlazeBuybackAuthorizationsLoader(PipelineLoader):
    """A pipeline loader for the ``BuybackAuth`` dataset that loads
    data from a blaze expression.

    Parameters
    ----------
    expr : Expr
        The expression representing the data to load.
    resources : dict, optional
        Mapping from the atomic terms of ``expr`` to actual data resources.
    odo_kwargs : dict, optional
        Extra keyword arguments to pass to odo when executing the expression.
    data_query_time : time, optional
        The time to use for the data query cutoff.
    data_query_tz : tzinfo or str
        The timezeone to use for the data query cutoff.

    Notes
    -----
    The expression should have a tabular dshape of::

       Dim * {{
           {SID_FIELD_NAME}: int64,
           {TS_FIELD_NAME}: datetime,
           {BUYBACK_ANNOUNCEMENT_FIELD_NAME}: ?datetime,
           {SHARE_COUNT_FIELD_NAME}: ?float64,
           {VALUE_FIELD_NAME}: ?float64
       }}

    Where each row of the table is a record including the sid to identify the
    company, the timestamp where we learned about the announcement, the
    date when the buyback was announced, the share count, and the value.

    If the '{TS_FIELD_NAME}' field is not included it is assumed that we
    start the backtest with knowledge of all announcements.
    """
    __doc__ = __doc__.format(
        TS_FIELD_NAME=TS_FIELD_NAME,
        SID_FIELD_NAME=SID_FIELD_NAME,
        BUYBACK_ANNOUNCEMENT_FIELD_NAME=BUYBACK_ANNOUNCEMENT_FIELD_NAME,
        SHARE_COUNT_FIELD_NAME=SHARE_COUNT_FIELD_NAME,
        VALUE_FIELD_NAME=VALUE_FIELD_NAME
    )

    _expected_fields = frozenset({
        TS_FIELD_NAME,
        SID_FIELD_NAME,
        BUYBACK_ANNOUNCEMENT_FIELD_NAME,
        SHARE_COUNT_FIELD_NAME,
        VALUE_FIELD_NAME
    })

    @preprocess(data_query_tz=optionally(ensure_timezone))
    def __init__(self,
                 expr,
                 resources=None,
                 odo_kwargs=None,
                 data_query_time=None,
                 data_query_tz=None,
                 dataset=BuybackAuthorizations):
        dshape = expr.dshape

        if not istabular(dshape):
            raise ValueError(
                'expression dshape must be tabular, got: %s' % dshape,
            )

        expected_fields = self._expected_fields
        self._expr = bind_expression_to_resources(
            expr[list(expected_fields)],
            resources,
        )
        self._odo_kwargs = odo_kwargs if odo_kwargs is not None else {}
        self._dataset = dataset
        check_data_query_args(data_query_time, data_query_tz)
        self._data_query_time = data_query_time
        self._data_query_tz = data_query_tz

    def load_adjusted_array(self, columns, dates, assets, mask):
        data_query_time = self._data_query_time
        data_query_tz = self._data_query_tz
        lower_dt, upper_dt = normalize_data_query_bounds(
            dates[0],
            dates[-1],
            data_query_time,
            data_query_tz,
        )

        raw = ffill_query_in_range(
            self._expr,
            lower_dt,
            upper_dt,
            self._odo_kwargs,
        )
        sids = raw.loc[:, SID_FIELD_NAME]
        raw.drop(
            sids[~sids.isin(assets)].index,
            inplace=True
        )
        if data_query_time is not None:
            normalize_timestamp_to_query_time(
                raw,
                data_query_time,
                data_query_tz,
                inplace=True,
                ts_field=TS_FIELD_NAME,
            )

        gb = raw.groupby(SID_FIELD_NAME)

        # Transform to input needed for BuybackAuthorizationsLoader (dict[sid
        #  -> pd.DataFrame])
        sid_to_dfs = {sid: raw.loc[group] for sid, group in gb.groups.iteritems()}

        return BuybackAuthorizationsLoader(
            dates,
            sid_to_dfs,
            dataset=self._dataset,
        ).load_adjusted_array(columns, dates, assets, mask)
