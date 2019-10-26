# -*- coding: utf-8 -*-
from datetime import datetime, time
from collections import deque
from itertools import chain
from math import sqrt, floor, ceil
import numpy as np

from flyerbots.strategy import Strategy
from flyerbots.indicator import *
from flyerbots.utils import dotdict, stop_watch

no_trade_time_range = [
    (time(18, 55), time(19, 55)), # JST 03:55-04:55 Bitflyerメンテナンスタイム
]

def stdev(source):
    average = sum(source)/len(source)
    average_sq = sum(p**2 for p in source) / len(source)
    variance = average_sq - (average * average)
    return sqrt(variance), average

def zscore(source):
    average = sum(source)/len(source)
    average_sq = sum(p**2 for p in source) / len(source)
    variance = average_sq - (average * average)
    std = sqrt(variance)
    return (source[-1]-average)/std if std else 0

def flooring(price, q=50):
    return int(floor(price/q)*q)

def ceiling(price, q=50):
    return int(ceil(price/q)*q)

class elephant:

    def __init__(self):
        # 200万まで配列で確保
        self.allboard = np.zeros(2000000)

    def board_update(self, board):
        # 板上書き
        self.mid_price = int(board['mid_price'])
        for b in board['bids']:
            self.allboard[min(int(b['price']),2000000-1)] = b['size']
        for b in board['asks']:
            self.allboard[min(int(b['price']),2000000-1)] = b['size']

    def find_target_price(self, position_size):
        # ask_depth = np.sum(self.allboard[self.mid_price+1:self.mid_price+3000])
        # bid_depth = np.sum(self.allboard[self.mid_price-3000:self.mid_price:])

        # 板厚・ポジションに合わせて買い指値決定
        sig = []
        if position_size<0:
            sig.append((1, 1.0, 0.0, 0.01))
        sig.append((2, 5.0, 0.0, 0.01))
        # if bid_depth>=ask_depth:
        #     sig.append((2, 3.0, 0.01))
        # else:
        #     sig.append((2, 3.0, 0.00))
        self.bids = []
        target = self.mid_price
        for id,d,b,size in sig:
            depth = 0
            while True:
                target -= 1
                bid = self.allboard[target]
                depth += bid
                if (d>0 and depth>d) or (b>0 and bid>=b):
                    break
            self.bids.append(('L'+str(id), target+1, size, depth))

        # 板厚・ポジションに合わせて売り指値決定
        sig = []
        if position_size>0:
            sig.append((1, 1.0, 0.0, 0.01))
        sig.append((2, 5.0, 0.0, 0.01))
        # if ask_depth>=bid_depth:
        #     sig.append((2, 3.0, 0.01))
        # else:
        #     sig.append((2, 3.0, 0.00))
        self.asks = []
        target = self.mid_price
        for id,d,a,size in sig:
            depth = 0
            while True:
                target += 1
                ask = self.allboard[target]
                depth += ask
                if (d>0 and depth>d) or (ask>=a):
                    break
            self.asks.append(('S'+str(id), target-1, size, depth))

        # # 指値位置表示
        for a in reversed(self.asks):
            logger.info('{0} {1} {2} {3}'.format(*a))
        logger.info('M {0}'.format(self.mid_price))
        for b in self.bids:
            logger.info('{0} {1} {2} {3}'.format(*b))

    def setup(self, strategy):
        # APIで板初期化
        self.board_update(strategy.fetch_order_book())
        # 板ストリーム購読開始
        self.ep = strategy.streaming.get_endpoint(strategy.settings.symbol, ['board'])

    def loop(self, ticker, ohlcv, strategy, **other):

        # メンテナンス時刻
        t = datetime.utcnow().time()
        coffee_break = False
        for s, e in no_trade_time_range:
            if t >= s and t <= e:
                # logger.info('Coffee break ...')
                coffee_break = True
                break

        # 板更新
        boards = self.ep.get_boards()
        for b in boards:
            self.board_update(b)

        # エントリー
        if not coffee_break:
            # 遅延評価
            delay = ohlcv.distribution_delay[-1]

            # 指値位置計算
            self.find_target_price(strategy.position_size)

            # ポジション価格帯
            restrict_range = {}
            # buy_pos = [p for p in strategy.position.all if p['side']=='BUY']
            # sell_pos = [p for p in strategy.position.all if p['side']=='SELL']
            # restrict_range = {flooring(p['price']):p['size'] for p in buy_pos}
            # restrict_range.update({ceiling(p['price']):p['size']*-1 for p in sell_pos})

            for myid,price,size,_ in reversed(self.asks):
                target = ceiling(price)
                s = restrict_range.get(target,0)
                if s >= 0 and size>0:
                    strategy.order(myid, 'sell', qty=size, limit=int(price), minute_to_expire=1,
                        seconds_to_keep_order=5)
                    restrict_range[target] = size*-1
                else:
                    strategy.cancel(myid)

            for myid,price,size,_ in reversed(self.bids):
                target = flooring(price)
                s = restrict_range.get(target,0)
                if s <= 0 and size>0:
                    strategy.order(myid, 'buy', qty=size, limit=int(price), minute_to_expire=1,
                        seconds_to_keep_order=5)
                    restrict_range[target] = size
                else:
                    strategy.cancel(myid)
        else:
            strategy.cancel_order_all()
            strategy.close_position()


if __name__ == "__main__":
    import settings
    import logging
    import logging.config

    logging.config.dictConfig(settings.loggingConf('elephant.log'))
    logger = logging.getLogger("elephant")

    bot = elephant()
    strategy = Strategy(yourlogic=bot.loop, yoursetup=bot.setup, interval=5)
    strategy.settings.apiKey = settings.apiKey
    strategy.settings.secret = settings.secret
    strategy.settings.disable_rich_ohlcv = True
    strategy.settings.max_ohlcv_size = 10*3
    strategy.risk.max_position_size = 0.02
    strategy.start()
