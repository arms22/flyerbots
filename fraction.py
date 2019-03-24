# -*- coding: utf-8 -*-
from flyerbots.strategy import Strategy
from flyerbots.indicator import *
from math import floor, ceil
from datetime import datetime, time

suspension_period = [
    (time(18, 55), time(19, 55)), # JST 03:55-04:55 Bitflyerメンテナンスタイム
]

def flooring(price, q):
    return int(floor(price/q)*q)

def ceiling(price, q):
    return int(ceil(price/q)*q)

class fraction:

    def __init__(self):
        pass

    def loop(self, ticker, ohlcv, strategy, **other):

        # メンテナンス時刻
        t = datetime.utcnow().time()
        coffee_break = False
        for s, e in suspension_period:
            if t >= s and t <= e:
                logger.info('Coffee break ...')
                coffee_break = True
                break

        if not coffee_break:

            # 51円値幅で指値バラマキ
            bid = ticker.best_bid
            ask = ticker.best_ask
            C = ohlcv.close[-1]
            buy = flooring(C-25.5,51)
            sell = ceiling(C+25.5,51)
            lot = 0.01
            for i in range(1):
                bp = int(buy-i*51)
                sp = int(sell+i*51)
                strategy.order(str(bp), 'buy', qty=lot, limit=bp, minute_to_expire=1)
                strategy.order(str(sp), 'sell', qty=lot, limit=sp, minute_to_expire=1)

            # 損切り
            for p in strategy.positions:
                side, price, size = p['side'], p['price'], p['size']
                if size>0.01:
                    if side=='buy':
                        pnl = bid - price
                        if pnl<-102:
                            strategy.order('C'+str(price), 'sell', qty=size)
                    else:
                        pnl = price-ask
                        if pnl<-102:
                            strategy.order('C'+str(price), 'buy', qty=size)
        else:
            strategy.cancel_order_all()
            strategy.close_position()


if __name__ == "__main__":
    import settings
    import logging
    import logging.config

    logging.config.dictConfig(settings.loggingConf('fraction.log'))
    logger = logging.getLogger("fraction")

    strategy = Strategy(fraction().loop, 0.5)
    strategy.settings.apiKey = settings.apiKey
    strategy.settings.secret = settings.secret
    strategy.settings.disable_rich_ohlcv = True
    strategy.risk.max_position_size = 0.1
    strategy.start()
