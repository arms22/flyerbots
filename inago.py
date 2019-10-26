# -*- coding: utf-8 -*-
from flyerbots.strategy import Strategy
from flyerbots.indicator import *
from datetime import datetime, time

no_trade_time_range = [
    (time(18,55), time(19, 55)),# JST 03:55-04:55 Bitflyerメンテナンスタイム
]

class inago:

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
            # イナゴ指標
            buychg = ohlcv.high[-1]-ohlcv.high[-4]
            sellchg = ohlcv.low[-1]-ohlcv.low[-4]

            # quadratic1
            maxsize = 0.05
            delta_pos = strategy.position_size
            if delta_pos>=maxsize:
                buysize = 0
            elif delta_pos<-maxsize:
                buysize = maxsize
            else:
                buysize = (-maxsize/4)*(((delta_pos+maxsize)/maxsize)**2)+maxsize
            if delta_pos<=-maxsize:
                sellsize = 0
            elif delta_pos>maxsize:
                sellsize = maxsize
            else:
                sellsize = (-maxsize/4)*(((-delta_pos+maxsize)/maxsize)**2)+maxsize

            buy = ticker.best_bid
            sell = ticker.best_ask
            if buysize>=0.01 and buychg>500:
                strategy.order('L', 'buy', qty=buysize, limit=buy)
            if sellsize>=0.01 and sellchg<-500:
                strategy.order('S', 'sell', qty=sellsize, limit=sell)

if __name__ == "__main__":
    import settings
    import logging
    import logging.config

    logging.config.dictConfig(settings.loggingConf('inago.log'))
    logger = logging.getLogger("inago")

    strategy = Strategy(inago().loop, 5)
    strategy.settings.apiKey = settings.apiKey
    strategy.settings.secret = settings.secret
    strategy.settings.disable_rich_ohlcv = True
    strategy.risk.max_position_size = 0.1
    strategy.start()
