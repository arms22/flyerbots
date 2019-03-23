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
            self.allboard[int(b['price'])] = b['size']
        for b in board['asks']:
            self.allboard[int(b['price'])] = b['size']

    def find_target_price(self, buy_vol_std, bid_vol_avg, sell_vol_std, sell_vol_avg):
        # pair = [(0.5, 0.02), (1, 0.02), (2, 0.02), (3, 0.02)]
        pair = [(1, 5, 0.02), (2, 5, 0.02), (3, 5, 0.02)]

        # 売りの強さに合わせて指値位置を調節
        # sig = [(i, sell_vol_avg+sell_vol_std*i, s) for i,s in pair]
        sig = pair
        self.bids = []
        target = self.mid_price
        for i,d,s in sig:
            depth = 0
            while True:
                target -= 1
                depth = self.allboard[target]
                if depth>d:
                    break
            self.bids.append(('L'+str(i), target+1, s, depth))

        # 買いの強さに合わせて指値位置を調節
        # sig = [(i, bid_vol_avg+buy_vol_std*i, s) for i,s in pair]
        sig = pair
        self.asks = []
        target = self.mid_price
        for i,d,s in sig:
            depth = 0
            while True:
                target += 1
                depth = self.allboard[target]
                if depth>d:
                    break
            self.asks.append(('S'+str(i), target-1, s, depth))

        # 指値位置表示
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

    def loop(self, ticker, ohlcv, position, strategy, **other):

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
        if 1:#not coffee_break:
            # 遅延評価
            delay = ohlcv.distribution_delay[-1]

            # 指値位置計算
            buy_vol_std, buy_vol_avg = stdev(ohlcv.buy_volume[-4*10:])
            sell_vol_std, sell_vol_avg = stdev(ohlcv.sell_volume[-4*10:])
            self.find_target_price(buy_vol_std, buy_vol_avg, sell_vol_std, sell_vol_avg)

            # ポジション価格帯
            buy_pos = [p for p in position.all if p['side']=='BUY']
            sell_pos = [p for p in position.all if p['side']=='SELL']
            restrict_range = {flooring(p['price']):p['size'] for p in buy_pos}
            restrict_range.update({ceiling(p['price']):p['size']*-1 for p in sell_pos})

            for ask in reversed(self.asks):
                target = ceiling(ask[1])
                s = restrict_range.get(target,0)
                if s >= 0:
                    strategy.order(ask[0], 'sell', qty=ask[2], limit=int(ask[1]), minute_to_expire=1)
                    restrict_range[target] = ask[2]*-1
                else:
                    strategy.cancel(ask[0])

            for bid in reversed(self.bids):
                target = flooring(bid[1])
                s = restrict_range.get(target,0)
                if s <= 0:
                    strategy.order(bid[0], 'buy', qty=bid[2], limit=int(bid[1]), minute_to_expire=1)
                    restrict_range[target] = bid[2]
                else:
                    strategy.cancel(bid[0])

            logger.info(f'{delay} {buy_vol_avg:.1f}({buy_vol_std:.1f})/{sell_vol_avg:.1f}({sell_vol_std:.1f})/')
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
    strategy.risk.max_position_size = 0.1
    strategy.start()
