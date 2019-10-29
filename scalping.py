# -*- coding: utf-8 -*-
from flyerbots.strategy import Strategy
from flyerbots.indicator import *
from datetime import datetime, time
from time import time as ttime
from statistics import mean

no_trade_time_range = [
    (time(15,55), time(21, 5)),# JST 00:55-06:05 Bitflyerメンテナンスタイム
]

class scalping:

    def __init__(self):
        self.order_keep_count = 2

    def loop(self, ticker, ohlcv, strategy, **other):
        if len(ohlcv.close)<5:
            return

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
            # 遅延
            delay = sorted(ohlcv.distribution_delay[-3:])[1]
            if delay>1:
                strategy.cancel_order_all()
                return

            # 注文待機
            self.order_keep_count -= 1
            if self.order_keep_count>0:
                return
            self.order_keep_count = 2

            # 指標
            C = ohlcv.close[-1]
            index = C/mean(ohlcv.average[-45:])-1

            maxsize = 0.05
            buysize = sellsize = maxsize
            long_size = max(strategy.position_size,0)
            short_size = max(-strategy.position_size,0)
            if long_size>=maxsize:
                buysize = 0
            if short_size>=maxsize:
                sellsize = 0

            buy = int(C-C*0.00025)
            sell = int(C+C*0.00025)
            if buysize>=0.01 and index>0.00005:
                strategy.order('L', 'buy', qty=buysize, limit=buy, minute_to_expire=1)
            else:
                strategy.cancel('L')
            if sellsize>=0.01 and index<-0.00005:
                strategy.order('S', 'sell', qty=sellsize, limit=sell, minute_to_expire=1)
            else:
                strategy.cancel('S')

            buy = int(C-C*0.0005)
            sell = int(C+C*0.0005)
            if long_size>=0.01:
                strategy.order('Lc', 'sell', qty=long_size, limit=sell, minute_to_expire=1)
            else:
                strategy.cancel('Lc')
            if short_size>=0.01:
                strategy.order('Sc', 'buy', qty=short_size, limit=buy, minute_to_expire=1)
            else:
                strategy.cancel('Lc')
        else:
            strategy.cancel_order_all()
            strategy.close_position()

if __name__ == "__main__":
    import settings
    import logging
    import logging.config

    logging.config.dictConfig(settings.loggingConf('scalping.log'))
    logger = logging.getLogger("scalping")

    strategy = Strategy(scalping().loop, 1)
    strategy.settings.apiKey = settings.apiKey
    strategy.settings.secret = settings.secret
    strategy.settings.use_lazy_ohlcv = True
    strategy.settings.timeframe = 1
    strategy.settings.disable_rich_ohlcv = True
    strategy.risk.max_position_size = 0.05
    strategy.start()
