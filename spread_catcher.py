# -*- coding: utf-8 -*-
import datetime
import time
from flyerbots.strategy import Strategy
from flyerbots.indicator import *


class spcatcher:

    def __init__(self):
        self.period = 6
        self.wait = 3

    def loop(self, ticker, ohlcv, strategy, **other):

        if self.wait:
            self.wait -= 1
            return

        ltp = ticker.ltp
        bid = min(ohlcv.low[-1],ohlcv.low[-2])
        ask = max(ohlcv.high[-1],ohlcv.high[-2])
        spr = ask - bid

        # トレンド指標計算
        vol = ohlcv.volume[-1]
        volma = sma(ohlcv.volume, self.period)[-1]
        volimb = sma(ohlcv.volume_imbalance, self.period)[-1]

        logger.info('ltp {ltp} bid/ask {bid}/{ask}({spr})'.format(**locals()))

        # システムメンテナンス
        t = datetime.datetime.utcnow().time()
        if (t >= datetime.time(18, 55)) and (t <= datetime.time(19, 55)):
            logger.info('Maintenance ...')
            strategy.cancel('S')
            strategy.cancel('L')
            if strategy.position_size > 0:
                strategy.order('L close', 'sell', qty=strategy.position_size)
            elif strategy.position_size < 0:
                strategy.order('S close', 'buy', qty=-strategy.position_size)
            return

        qty_lot = 0.05

        # トレンド発生
        if volma>220 or vol>220:
            strategy.cancel('L')
            strategy.cancel('S')
            # ドテン
            if volimb>10:
                strategy.entry('L tf', 'buy', qty=qty_lot)
            elif volimb<-10:
                strategy.entry('S tf', 'sell', qty=qty_lot)
        else:
            spr_target = 50
            if spr >= spr_target or strategy.position_size < 0:
                strategy.order('L', 'buy', qty=qty_lot, limit=bid+1, minute_to_expire=1)
            else:
                strategy.cancel('L')
            if spr >= spr_target or strategy.position_size > 0:
                strategy.order('S', 'sell', qty=qty_lot, limit=ask-1, minute_to_expire=1)
            else:
                strategy.cancel('S')

if __name__ == "__main__":
    import settings
    import logging
    import logging.config

    logging.config.dictConfig(settings.loggingConf('spcatcher.log'))
    logger = logging.getLogger("spcatcher")

    strategy = Strategy(spcatcher().loop, 5)
    # strategy.settings.symbol = 'BTCJPY28DEC2018'
    strategy.settings.apiKey = settings.apiKey
    strategy.settings.secret = settings.secret
    strategy.settings.show_last_n_orders = 10
    strategy.risk.max_position_size = 0.05
    strategy.start()
