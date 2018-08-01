import talib
import numpy as np
import pandas as pd
import math
'''
from quantopian.algorithm import attach_pipeline, pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.factors import AverageDollarVolume
from quantopian.pipeline.filters import QTradableStocksUS # Q1500US
'''

from zipline.api import *
from zipline.errors import SymbolNotFound
import logbook
log = logbook.Logger('algo')

import alpaca_trade_api as tradeapi
api = tradeapi.REST()

import logging

logging.getLogger().setLevel(logging.DEBUG)

from contextlib import contextmanager

@contextmanager
def retry(n=3, catch=Exception):
    for i in range(n):
        try:
            yield i
        except catch as e:
            import time
            print(e)
            time.sleep(3 ** i)
            if i == n - 1:
                raise e
        else:
            raise StopIteration()

def initialize(context):

    set_long_only()
    # set_commission(commission.PerTrade(cost=0))

    # attach_pipeline(make_pipeline(), 'my_pipeline')

'''
def make_pipeline():

    base_universe = QTradableStocksUS() # Q1500US()
    yesterday_close = USEquityPricing.close.latest
    prices_over_one = (yesterday_close > 2.0)
    prices_under_ten = (yesterday_close < 13.0)

    pipe = Pipeline(
        screen = base_universe & prices_over_one & prices_under_ten,
        # screen = prices_over_one & prices_under_ten,
        columns = {
            'close': yesterday_close,
        }
    )
    return pipe
'''

def filter_assets():
    assets = api.list_assets()
    symbols = [a.symbol for a in assets]
    results = []
    batch_size = 200
    now = pd.Timestamp.now(tz='America/New_York')
    end_dt = now.floor('1D') - pd.Timedelta('1 minute')
    for i in range(0, len(symbols), batch_size):
        with retry():
            bars_list = api.list_bars(symbols[i:i+batch_size], '1D', limit=5, end_dt=end_dt.isoformat())
        for bars in bars_list:
            try:
                df = bars.df
            except:
                continue
            
            if pd.Timestamp.now() - pd.Timestamp(df.index[-1]) > pd.Timedelta('6 day'):
                log.info('{} is too old'.format(bars.symbol))
                continue
            if len(df.volume) < 1:
                continue
            if df.volume[-1] <= 300000:
                continue
            if df.close[-1] >= 2.0 and df.close[-1] < 13.0:
                try:
                    zp_asset = symbol(bars.symbol)
                except SymbolNotFound as exc:
                    log.error(exc)
                results.append(zp_asset)
    log.info('today\'s stock = {}'.format([
        a.symbol for a in results
    ]))
    return results
        
def init_context(context, data):
    context.assets = filter_assets()
    # print(data.history(context.assets[:20], "close", 5, "1d").dropna())
    context.stop_prices = {}
    context.target_prices = {}
    context.entry_timestamps = {}

def before_trading_start(context, data):

    init_context(context, data)
    if pd.Timestamp.now(tz='America/New_York').time() >= pd.Timestamp('9:45').time():
        mark_15h(context, data)
    else:
        schedule_function(mark_15h,
                        date_rules.every_day(),
                        time_rules.market_open(minutes=15))

    for minutes in range(16, 30, 1):
        schedule_function(buy,
                          date_rules.every_day(),
                          time_rules.market_open(minutes=minutes))

    for minutes in range(24, 360, 1):
        schedule_function(sell_check,
                          date_rules.every_day(),
                          time_rules.market_open(minutes=minutes))

    schedule_function(clean,
                      date_rules.every_day(),
                      time_rules.market_open(minutes=1))
    schedule_function(clean,
                      date_rules.every_day(),
                      time_rules.market_close(minutes=25))
    # context.output = pipeline_output('my_pipeline')
    # context.assets = context.output.index
    print('before_trading_start done')

def mark_15h(context, data):

    now = pd.Timestamp.now(tz='America/New_York')
    market_open = now.replace(hour=9, minute=30)
    first_15min = data.history(context.assets, 'price', 500, '1m').dropna()
    first_15min = first_15min[market_open:market_open+pd.Timedelta('15min')]
    context.high_15min = first_15min.max()
    # print(context.high_15min)

def buy(context, data):

    try:
        with retry(5):
            yesterday = pd.Timestamp.now(tz='America/New_York').floor('1D') - pd.Timedelta('1minute')
            last_closes = data.history(context.assets, "close", 5, "1d").dropna()[:yesterday].iloc[-1]
            current_prices = data.history(context.assets, "price", 1, "1m").dropna().iloc[-1]
    except Exception as e:
        log.warn(e)
        return
    # current_prices = data.current(context.assets, "price")

    open_orders = get_open_orders()
    positions = context.portfolio.positions

    pvalue = context.portfolio.portfolio_value
    log.info('pvalue = {}'.format(pvalue))
    log.info('filtering {} stocks'.format(len(context.assets)))
    for stock in context.assets:
        if stock in open_orders: #  or not data.can_trade(stock):
            continue
        if stock in positions:
            continue
        current = current_prices[stock]
        last_close = last_closes[stock]
        pct_change = (current - last_close) / last_close
        # print('{}: {:.02f}%'.format(stock.symbol, pct_change * 100))
        # if (pd.Series.pct_change(history_1d[stock])[-1]) <= 0.04:
        if pct_change <= 0.04:
            continue
        # if (history_1d_vol[stock])[-1] <= 30000.0:
        #     continue
        high_15min = context.high_15min[stock]
        if current > high_15min:
            stop_price = find_stop(stock, context, data)
            if stop_price is None:
                print('could not find stop_price for {}'.format(stock.symbol))
                continue
            if stop_price >= current:
                print('{} >= current'.format(stop_price, current))
                continue
            context.stop_prices[stock] = stop_price
            context.target_prices[stock] = current + (current - stop_price) * 3
            context.entry_timestamps[stock] = get_datetime()
            risk = 0.02
            shares = pvalue * risk // (current - stop_price)
            
            ## DEBUG ##
            if shares * current > 100:
                shares = 100 // current
            if shares == 0:
                log.info('shares == 0 for {}'.format(stock.symbol))
                continue
            order(stock, shares)
            log.info('buy {}: current={}, high_15min={}, shares={}, stop={}'.format(
                    stock, current, high_15min, shares, stop_price))

def find_stop(stock, context, data):
    series = data.history(stock, "low", 100, "1m").dropna()
    now = get_datetime(series.index.tz)
    series = series[now.floor('1D'):]
    diff = np.diff(series.values)
    low_index = np.where((diff[:-1] <= 0) & (diff[1:] > 0))[0] + 1
    if len(low_index) > 0:
        return series[low_index[-1]] - 0.01
    return None

def sell_check(context, data):

    #prices = data.history(context.assets, 'price', 3000, '1m').resample(
    #    '5min').last().dropna()
    try:
        with retry(5):
            prices = data.history(context.assets, 'price', 3000, '1m').dropna()
    except:
        print('retry exceeded')
        return

    # log.info('{}'.format(prices[symbol('JCP')][-100:]))

    open_orders = get_open_orders()
    for stock, position in context.portfolio.positions.items():
        if stock in open_orders:
            continue
        if stock not in prices:
            continue
        if stock not in context.stop_prices:
            log.warn('{} not in stop prices'.format(stock.symbol))
            continue
        macd_raw, signal, hist = talib.MACD(prices[stock].values, fastperiod=13,
                                        slowperiod=21, signalperiod=8)

        price = prices[stock][-1]
        typ = None
        stop_price = context.stop_prices[stock]
        if hist[-1] < 0:
            typ = 'MACD'
        if stop_price >= price:
            typ = 'stop'
        entry_timestamp = context.entry_timestamps[stock]
        prices_since = prices[stock][entry_timestamp:]
        hit_target = (prices_since >= context.target_prices[stock]).any()
        if hit_target and np.diff(macd_raw)[-1] <= 0.0:
            typ = 'profit'
        if typ is not None:
            cost_basis = position.cost_basis
            log.info('exit({}): {} {} shares at {} <- {:.2f} ({:.2f}%)'.format(
                    typ, stock, position.amount, price, cost_basis,
                    (price - cost_basis) / cost_basis * 100))
            order(stock, -position.amount)

def clean(context, data):

    for stock, position in context.portfolio.positions.items():
        log.info('clean: {}'.format(stock))
        order(stock, -position.amount)
