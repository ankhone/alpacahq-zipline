from zipline.pipeline.factors import CustomFactor
from zipline.pipeline.filters import CustomFilter

import alpaca_trade_api as tradeapi

def polygon_company(symbol):
    api = tradeapi.REST()
    return api.polygon.get('/meta/symbols/{}/company'.format(symbol))


import concurrent.futures
import hashlib
import iexfinance
import logbook
import numpy as np
import pandas as pd
import os
import pickle

log = logbook.Logger(__name__)

def daily_cache(filename):
    kwd_mark = '||'

    def decorator(func):
        def wrapper(*args, **kwargs):
            key = args + (kwd_mark,) + tuple(sorted(kwargs.items()))
            hash = hashlib.md5()
            hash.update(str(key).encode('utf-8'))
            hash.update(pd.Timestamp.utcnow().strftime('%Y-%m-%d').encode('utf-8'))
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
def polygon_companies(symbols, output='pandas'):
    result = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        tasks = {
            executor.submit(polygon_company, symbol): symbol
            for symbol in symbols
        }
        
        total_count = len(symbols)
        report_percent = 10
        processed = 0
        for task in concurrent.futures.as_completed(tasks):
            symbol = tasks[task]
            task_result = task.result()
            result[symbol] = task_result
            processed += 1 # len(part_result)
            percent = processed / total_count * 100
            if percent >= report_percent:
                print('{:.2f}% completed'.format(percent))
                last_print_percent = percent
                report_percent = (percent + 10.0) // 10 * 10

    if output == 'pandas':
        return pd.DataFrame(result)
    return result


@daily_cache(filename='iex_financials.pkl')
def iex_financials(symbols):
    return get_stockdata(symbols, 'get_financials', output='raw')


def get_stockdata(symbols, method, output='pandas'):
    '''Get stock data (key stats and previous) from IEX.
    Just deal with IEX's 99 stocks limit per request.
    '''

    def fetch(symbols):
        return getattr(iexfinance.Stock(symbols), method)()

    iex_symbols = [
        symbol['symbol'] for symbol in iexfinance.get_available_symbols()
    ]

    symbols = list(set(symbols) & set(iex_symbols))
    result = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        tasks = []
        partlen = 99
        for i in range(0, len(symbols), partlen):
            part = symbols[i:i + partlen]
            task = executor.submit(fetch, part)
            tasks.append(task)

        total_count = len(symbols)
        report_percent = 10
        processed = 0
        for task in concurrent.futures.as_completed(tasks):
            part_result = task.result()
            result.update(part_result)
            processed += len(part_result)
            percent = processed / total_count * 100
            if percent >= report_percent:
                log.debug('get_stockdata: {:.2f}% completed'.format(percent))
                last_print_percent = percent
                report_percent = (percent + 10.0) // 10 * 10

    if output == 'pandas':
        return pd.DataFrame(result)
    return result


def get_us_company(asset_finder):

    class USCompany(CustomFilter):
        inputs = ()
        window_length = 1

        def compute(self, today, sids, out, *inputs):
            assets = asset_finder.retrieve_all(sids)
            symbols = [a.symbol for a in assets]
            ret = polygon_companies(sorted(symbols), output='raw')
            ary = np.array([
                a.symbol in ret and 
                'country' in ret[a.symbol] and 
                [ret[a.symbol]['country'] == 'us']
                for a in assets
            ], dtype=bool)
            out[:] = ary
            

    return USCompany()


def get_company_with_financials(asset_finder):

    class CompanyWithFinancials(CustomFilter):
        inputs = ()
        window_length = 1

        def compute(self, today, sids, out, *inputs):
            assets = asset_finder.retrieve_all(sids)
            symbols = [a.symbol for a in assets]
            ret = iex_financials(symbols)
            ary = np.array([
                a.symbol in ret and
                len(ret[a.symbol]) > 0 and
                'totalRevenue' in ret[a.symbol][0] and
                ret[a.symbol][0]['totalRevenue']
                for a in assets
            ], dtype=bool)
            out[:] = ary

    return CompanyWithFinancials()


class IsPrimaryShare(CustomFactor):

    def compute(self, today, assets, out, highs, lows):
        pass
