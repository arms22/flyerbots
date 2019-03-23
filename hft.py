# -*- coding: utf-8 -*-
from datetime import datetime, time
from collections import deque
from itertools import chain
from math import sqrt

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
    return sqrt(variance)

def zscore(source):
    average = sum(source)/len(source)
    average_sq = sum(p**2 for p in source) / len(source)
    variance = average_sq - (average * average)
    std = sqrt(variance)
    return (source[-1]-average)/std if std else 0

class hft:

    def __init__(self):
        pass

    def loop(self, ticker, ohlcv, strategy, **other):

        # メンテナンス時刻
        t = datetime.utcnow().time()
        coffee_break = False
        for s, e in no_trade_time_range:
            if t >= s and t <= e:
                logger.info('Coffee break ...')
                coffee_break = True
                break

        # エントリー
        if not coffee_break:
            mid = (ohlcv.high[-1]+ohlcv.low[-1]+ohlcv.close[-1])/3
            std = stdev(ohlcv.close[-60:])
            rng = max(std, 33)
            pof = (strategy.position_size/strategy.risk.max_position_size)*rng
            z = zscore(ohlcv.volume_imbalance)
            if z>0:
                buy = mid - pof
                sell = mid + rng*3
            elif z<-0:
                buy = mid - rng*3
                sell = mid - pof
            else:
                buy = mid - rng*3
                sell = mid + rng*3
            strategy.order('L', 'buy', qty=max(0.02 * z, 0.01), limit=int(buy), limit_mask=rng*0.5, minute_to_expire=1)
            strategy.order('S', 'sell', qty=max(0.02 * -z, 0.01), limit=int(sell), limit_mask=rng*0.5, minute_to_expire=1)
        else:
            strategy.cancel_order_all()
            strategy.close_position()


if __name__ == "__main__":
    import settings
    import logging
    import logging.config

    logging.config.dictConfig(settings.loggingConf('hft.log'))
    logger = logging.getLogger("hft")

    strategy = Strategy(hft().loop, 0.5)
    strategy.settings.apiKey = settings.apiKey
    strategy.settings.secret = settings.secret
    strategy.settings.disable_rich_ohlcv = True
    strategy.settings.max_ohlcv_size = 1000
    strategy.risk.max_position_size = 0.1
    strategy.start()
