# -*- coding: utf-8 -*-
from datetime import datetime, time
from math import sqrt
from flyerbots.strategy import Strategy
from flyerbots.indicator import *

no_trade_time_range = [
    (time( 7,55), time( 8, 5)), # JST 16:55-17:05
    (time( 8,55), time( 9, 5)), # JST 17:55-18:05
    (time( 9,55), time(10, 5)), # JST 18:55-19:05
    (time(10,55), time(11, 5)), # JST 19:55-20:05
    (time(11,55), time(12, 5)), # JST 20:55-21:05
    (time(12,55), time(13, 5)), # JST 21:55-22:05
    (time(13,55), time(14, 5)), # JST 22:55-23:05
    (time(14,55), time(15, 5)), # JST 23:55-00:05
    (time(15,55), time(23, 5)), # JST 00:55-08:05
    # (time(18,55), time(19, 55)), # JST 03:55-04:55 Bitflyerメンテナンスタイム
]

def stdev(src):
    average = sum(src) / len(src)
    average_sq = sum(p**2 for p in src) / len(src)
    var = average_sq - (average * average)
    dev = sqrt(var)
    return dev

def zscore(source):
    average = sum(source)/len(source)
    average_sq = sum(p**2 for p in source) / len(source)
    variance = average_sq - (average * average)
    std = sqrt(variance)
    return (source[-1]-average)/std if std else 0

class simple_market_maker:

    def __init__(self):
        self.core = self.smm_logic1
        # self.core = self.smm_logic2

    def smm_logic1(self, ticker, ohlcv, strategy, **kwargs):

        # 遅延評価
        dist = ohlcv.distribution_delay[-3:]
        delay = sorted(dist)[int(len(dist)/2)]

        # 指値幅計算
        spr = max(stdev(ohlcv.close), 100)
        pairs = [(0.04, spr*1.0, 3), (0.03, spr*0.5, 2), (0.02, spr*0.25, 1), (0.01, spr*0.125, 0)]
        maxsize = sum(p[0] for p in pairs)
        buymax = sellmax = strategy.position_size
        # mid = ohlcv.close[-1]
        mid = (ohlcv.high[-1]+ohlcv.low[-1]+ohlcv.close[-1])/3
        # mid = ohlcv.average[-1]
        z = zscore(ohlcv.volume_imbalance)
        ofs = z*9

        if delay>2.0:
            if strategy.position_size>=0.01:
                strategy.order('L close', 'sell', qty=0.01, limit=int(mid), minute_to_expire=1)
            elif strategy.position_size<=-0.01:
                strategy.order('S close', 'buy', qty=0.01, limit=int(mid), minute_to_expire=1)
            for pair in pairs:
                suffix = str(pair[2])
                strategy.cancel('L'+suffix)
                strategy.cancel('S'+suffix)
        else:
            strategy.cancel('L close')
            strategy.cancel('S close')
            for pair in pairs:
                suffix = str(pair[2])
                if buymax+pair[0] <= maxsize:
                    strategy.order('L'+suffix, 'buy', qty=pair[0], limit=int(mid-pair[1]-ofs), limit_mask=pair[1]*0.1, minute_to_expire=1)
                    buymax += pair[0]
                else:
                    strategy.cancel('L'+suffix)
                if sellmax-pair[0] >= -maxsize:
                    strategy.order('S'+suffix, 'sell', qty=pair[0], limit=int(mid+pair[1]+ofs), limit_mask=pair[1]*0.1, minute_to_expire=1)
                    sellmax -= pair[0]
                else:
                    strategy.cancel('S'+suffix)

    def smm_logic2(self, ticker, ohlcv, strategy, **kwargs):

        maxsize = strategy.risk.max_position_size
        buysize = sellsize = maxsize
        spr = max(stdev(ohlcv.close[-12*15:]), 200)
        mid = (ohlcv.high[-1]+ohlcv.low[-1]+ohlcv.close[-1])/3
        mid = mid - (strategy.position_size/maxsize)*spr*0.35
        ofs = ohlcv.trades[-1]*0.4
        buy = mid - spr*0.6 - ofs
        sell = mid + spr*0.6 + ofs

        if strategy.position_size < maxsize:
            strategy.order('L', 'buy', qty=buysize, limit=int(buy), minute_to_expire=1)
        else:
            strategy.cancel('L')
        if strategy.position_size > -maxsize:
            strategy.order('S', 'sell', qty=sellsize, limit=int(sell), minute_to_expire=1)
        else:
            strategy.cancel('S')

    def loop(self, ohlcv, ticker, strategy, **other):

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
            self.core(ticker, ohlcv, strategy)
        else:
            strategy.cancel_order_all()
            strategy.close_position()

if __name__ == "__main__":
    import settings
    import argparse
    import logging
    import logging.config

    logging.config.dictConfig(settings.loggingConf('simple_market_maker.log'))
    logger = logging.getLogger("simple_market_maker")

    strategy = Strategy(simple_market_maker().loop, 5)
    strategy.settings.apiKey = settings.apiKey
    strategy.settings.secret = settings.secret
    strategy.settings.max_ohlcv_size = 12*45
    strategy.settings.disable_rich_ohlcv = True
    strategy.risk.max_position_size = 0.1
    strategy.start()
