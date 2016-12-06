#
# Copyright 2014 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Note that part of the API is implemented in TradingAlgorithm as
# methods (e.g. order). These are added to this namespace via the
<<<<<<< HEAD
# decorator `api_methods` inside of algorithm.py.

# import zipline
from .finance import (commission, slippage)
from .utils import math_utils, events

from zipline.finance.slippage import (
=======
# decorator ``api_method`` inside of algorithm.py.
from .finance.asset_restrictions import (
    Restriction,
    StaticRestrictions,
    HistoricalRestrictions,
    RESTRICTION_STATES,
)
from .finance import commission, execution, slippage, cancel_policy
from .finance.cancel_policy import (
    NeverCancel,
    EODCancel
)
from .finance.slippage import (
>>>>>>> upstream/master
    FixedSlippage,
    VolumeShareSlippage,
)
from .utils import math_utils, events
from .utils.events import (
    date_rules,
    time_rules
)

<<<<<<< HEAD
from zipline.transforms import BatchTransform as batch_transform


=======
>>>>>>> upstream/master
__all__ = [
    'EODCancel',
    'FixedSlippage',
    'NeverCancel',
    'VolumeShareSlippage',
    'Restriction',
    'StaticRestrictions',
    'HistoricalRestrictions',
    'RESTRICTION_STATES',
    'cancel_policy',
    'commission',
    'date_rules',
    'events',
    'execution',
    'math_utils',
    'slippage',
    'time_rules'
]
