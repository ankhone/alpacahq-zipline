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

log = logbook.Logger(__name__)


def parallelize(mapfunc, workers=10, splitlen=10):

    def wrapper(symbols):
        result = {}
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=workers) as executor:
            tasks = []
            for i in range(0, len(symbols), splitlen):
                part = symbols[i:i + splitlen]
                task = executor.submit(mapfunc, part)
                tasks.append(task)

            total_count = len(symbols)
            report_percent = 10
            processed = 0
            for task in concurrent.futures.as_completed(tasks):
                task_result = task.result()
                result.update(task_result)
                processed += len(task_result)
                percent = processed / total_count * 100
                if percent >= report_percent:
                    log.debug('{}: {:.2f}% completed'.format(
                        mapfunc.__name__, percent))
                    report_percent = (percent + 10.0) // 10 * 10
        return result

    return wrapper


def daily_cache(filename):
    kwd_mark = '||'

    def decorator(func):
        def wrapper(*args, **kwargs):
            key = args + (kwd_mark,) + tuple(sorted(kwargs.items()))
            hash = hashlib.md5()
            hash.update(str(key).encode('utf-8'))
            hash.update(pd.Timestamp.utcnow().strftime(
                '%Y-%m-%d').encode('utf-8'))
            digest = hash.hexdigest()
            dirpath = '/root/.zipline/dailycache'
            os.makedirs(dirpath, exist_ok=True)
            filepath = os.path.join(dirpath, filename)
            if os.path.exists(filepath):
                try:
                    with open(filepath, 'rb') as fp:
                        ret = pickle.load(fp)
                        if ret['digest'] == digest:
                            return ret['body']
                        print('digest mismatch {} != {}'.format(
                            ret['digest'], digest
                        ))
                except Exception as e:
                    print('cache error {}'.format(e))
            body = func(*args, **kwargs)

            with open(filepath, 'wb') as fp:
                pickle.dump({
                    'digest': digest,
                    'body': body,
                }, fp)
            return body
        return wrapper
    return decorator


@daily_cache(filename='polygon_companies.pkl')
def polygon_companies(symbols):
    def fetch(symbols):
        api = tradeapi.REST()
        params = {
            'symbols': ','.join(symbols),
        }
        response = api.polygon.get('/meta/symbols/company', params=params)
        return {
            o['symbol']: o for o in response
        }

    return parallelize(fetch, workers=25, splitlen=50)(symbols)


@daily_cache(filename='polygon_financials.pkl')
def polygon_financials(symbols):
    def fetch(symbols):
        api = tradeapi.REST()
        params = {
            'symbols': ','.join(symbols),
        }
        return api.polygon.get('/meta/symbols/financials', params=params)

    return parallelize(fetch, workers=25, splitlen=50)(symbols)


@daily_cache(filename='iex_financials.pkl')
def iex_financials(symbols):
    def fetch(symbols):
        return iexfinance.Stock(symbols).get_financials()

    iex_symbols = [
        symbol['symbol'] for symbol in iexfinance.get_available_symbols()
    ]
    symbols = list(set(symbols) & set(iex_symbols))

    return parallelize(fetch, splitlen=99)(symbols)


class USCompany(CustomFilter):
    inputs = ()
    window_length = 1

    def __init__(self, *args, **kwargs):
        super(USCompany, self).__init__(*args, **kwargs)
        self._asset_finder = AssetFinderAlpaca()

    def compute(self, today, sids, out, *inputs):
        asset_finder = self._asset_finder
        assets = asset_finder.retrieve_all(sids)
        symbols = sorted([a.symbol for a in assets])
        ret = polygon_companies(symbols)
        ary = np.array([
            a.symbol in ret and
            'country' in ret[a.symbol] and
            [ret[a.symbol]['country'] == 'us']
            for a in assets
        ], dtype=bool)
        out[:] = ary


class CompanyWithFinancials(CustomFilter):
    inputs = ()
    window_length = 1

    def __init__(self, *args, **kwargs):
        super(CompanyWithFinancials, self).__init__(*args, **kwargs)
        self._asset_finder = AssetFinderAlpaca()

    def compute(self, today, sids, out, *inputs):
        asset_finder = self._asset_finder
        assets = asset_finder.retrieve_all(sids)
        symbols = sorted([a.symbol for a in assets])
        ret = iex_financials(symbols)
        ary = np.array([
            a.symbol in ret and
            len(ret[a.symbol]) > 0 and
            'totalRevenue' in ret[a.symbol][0] and
            ret[a.symbol][0]['totalRevenue']
            for a in assets
        ], dtype=bool)
        out[:] = ary


# def IsPrimaryShare():
#     return USCompany() & CompanyWithFinancials()


class IsPrimaryShare(CustomFilter):
    inputs = ()
    window_length = 1

    def __init__(self, *args, **kwargs):
        super(IsPrimaryShare, self).__init__(*args, **kwargs)
        self._asset_finder = AssetFinderAlpaca()

    def compute(self, today, sids, out, *inputs):
        asset_finder = self._asset_finder
        assets = asset_finder.retrieve_all(sids)
        symbols = sorted([a.symbol for a in assets])
        companies = polygon_companies(symbols)
        financials = polygon_financials(symbols)
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
