# -*- coding: utf-8 -*-
from flyerbots.strategy import Strategy
from flyerbots.indicator import *
from datetime import datetime, time
from statistics import mean

no_trade_time_range = [
    (time(18,55), time(19, 55)),# JST 03:55-04:55 Bitflyerメンテナンスタイム
]

class scalping:

    def __init__(self):
        pass

    def loop(self, ticker, ohlcv, strategy, **other):
        if len(ohlcv.close)<4:
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
            # 指標
            C = ohlcv.close[-1]
            index = C/mean(ohlcv.average[-10:])-1

            maxsize = 0.05
            buysize = sellsize = maxsize
            long_size = max(strategy.position_size,0)
            short_size = max(-strategy.position_size,0)
            if long_size>=maxsize:
                buysize = 0
            if short_size>=maxsize:
                sellsize = 0

            buy = int(C-C*0.0005)
            sell = int(C+C*0.0005)
            if buysize>=0.01 and index>0.0005:
                strategy.order('L', 'buy', qty=buysize, limit=buy, minute_to_expire=1)
            else:
                strategy.cancel('L')
            if sellsize>=0.01 and index<-0.0005:
                strategy.order('S', 'sell', qty=sellsize, limit=sell, minute_to_expire=1)
            else:
                strategy.cancel('S')

            buy = int(C-C*0.00095)
            sell = int(C+C*0.00095)
            if long_size>=0.01:
                strategy.order('Lc', 'sell', qty=long_size, limit=sell, minute_to_expire=1)
            else:
                strategy.cancel('Lc')
            if short_size>=0.01:
                strategy.order('Sc', 'buy', qty=short_size, limit=buy, minute_to_expire=1)
            else:
                strategy.cancel('Lc')

if __name__ == "__main__":
    import settings
    import logging
    import logging.config

    logging.config.dictConfig(settings.loggingConf('scalping.log'))
    logger = logging.getLogger("scalping")

    strategy = Strategy(scalping().loop, 5)
    strategy.settings.apiKey = settings.apiKey
    strategy.settings.secret = settings.secret
    strategy.settings.disable_rich_ohlcv = True
    strategy.risk.max_position_size = 0.05
    strategy.start()
