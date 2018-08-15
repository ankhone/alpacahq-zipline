import alpaca_trade_api as tradeapi
import concurrent.futures
import hashlib
import iexfinance
import logbook
import numpy as np
import pandas as pd
import os
import pickle

from zipline.assets.assets_alpaca import AssetFinderAlpaca
from zipline.pipeline.filters import CustomFilter
from zipline.pipeline.data import polygon


class IsPrimaryShare(CustomFilter):
    inputs = ()
    window_length = 1

    def compute(self, today, sids, out, *inputs):
        companies = polygon.companies()
        financials = polygon.financials()
        ary = np.array([
            a.symbol in companies and
            'country' in companies[a.symbol] and
            [companies[a.symbol].get('country') == 'us'] and
            a.symbol in financials and
            len(financials[a.symbol]) > 0 and
            financials[a.symbol][0].get('totalRevenue') is not None
            for a in assets
        ], dtype=bool)
        out[:] = ary
