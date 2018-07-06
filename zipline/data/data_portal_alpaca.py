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

from zipline.data.data_portal import DataPortal
from zipline.utils.memoize import weak_lru_cache
from logbook import Logger

import pandas as pd

log = Logger('DataPortalAlpaca')


class DataPortalAlpaca(DataPortal):

    def __init__(self, broker, asset_finder, trading_calendar):
        self.broker = broker
        self.asset_finder = asset_finder
        self.trading_calendar = trading_calendar

    def get_last_traded_dt(self, asset, dt, data_frequency):
        return self.broker.get_last_traded_dt(asset)

    def get_spot_value(self, assets, field, dt, data_frequency):
        return self.broker.get_spot_value(assets, field, dt, data_frequency)

    def get_splits(self, assets, dt):
        # right now data portal does not support splits, positions' shares are
        # automatically adjusted by brokerage.
        return None

    def get_adjustments(self, assets, field, dt, perspective_dt):
        # hack not to provide adjustment, as upstream provides adjusted pricing.
        return pd.Series([
            1 for i in assets
        ], index=assets)

    @weak_lru_cache(10)
    def _get_realtime_bars(self, assets, frequency, bar_count, end_dt):
        return self.broker.get_realtime_bars(
            assets, frequency, bar_count=bar_count)

    def get_history_window(self,
                           assets,
                           end_dt,
                           bar_count,
                           frequency,
                           field,
                           data_frequency,
                           ffill=True):

        # convert list of asset to tuple of asset to be hashable
        assets = tuple(assets)

        # Broker.get_realtime_history() returns the asset as level 0 column,
        # open, high, low, close, volume returned as level 1 columns.
        # To filter for field the levels needs to be swapped
        bars = self._get_realtime_bars(
            assets, frequency, bar_count=bar_count, end_dt=end_dt).swaplevel(0, 1, axis=1)

        ohlcv_field = 'close' if field == 'price' else field

        bars = bars[ohlcv_field]

        if ffill and field == 'price':
            # Simple forward fill is not enough here as the last ingested
            # value might be outside of the requested time window. That case
            # the time series starts with NaN and forward filling won't help.
            # To provide values for such cases we backward fill.
            # Backward fill as a second operation will have no effect if the
            # forward-fill was successful.
            bars.fillna(method='ffill', inplace=True)
            bars.fillna(method='bfill', inplace=True)

        return bars[-bar_count:]
