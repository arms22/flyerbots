# -*- coding: utf-8 -*-
from strategy import Strategy
from indicator import *
from utils import dotdict
from datetime import datetime

class BusyMonitor:

    def __init__(self):
        self.once = True

    def loop(self, ohlcv, ticker, board_state, strategy, **other):
        out = dotdict()
        out.health = board_state.health
        out.state = board_state.state
        out.datetime = datetime.utcnow()
        out.open = ohlcv.open[-1]
        out.high = ohlcv.high[-1]
        out.low = ohlcv.low[-1]
        out.close = ohlcv.close[-1]
        out.volume = ohlcv.volume[-1]
        out.average = ohlcv.average[-1]
        out.stdev = ohlcv.stdev[-1]
        out.trades = ohlcv.trades[-1]
        out.imbalance = ohlcv.imbalance[-1]
        out.volume_imbalance = ohlcv.volume_imbalance[-1]
        # out.opened_at = ohlcv.opened_at[-1]
        # out.closed_at = ohlcv.closed_at[-1]
        out.delay_seconds = ohlcv.delay_seconds[-1]
        if self.once:
            logger.info(','.join(str(v) for v in out.keys()))
            self.once = False
        logger.info(','.join(str(v) for v in out.values()))

if __name__ == "__main__":
    import settings
    import argparse
    import logging
    import logging.config

    logger = logging.getLogger("BusyMonitor")
    logger.setLevel(logging.INFO)

    handler = logging.handlers.TimedRotatingFileHandler(filename='busy_monitor.log', when='D', backupCount=7)
    logger.addHandler(handler)

    handler = logging.StreamHandler()
    logger.addHandler(handler)

    strategy = Strategy(BusyMonitor().loop, 10)
    strategy.settings.disable_rich_ohlcv = True
    # strategy.settings.realtime_graph = False
    # strategy.settings.symbol = 'BTC/JPY'
    # strategy.settings.apiKey = settings.apiKey
    # strategy.settings.secret = settings.secret
    # strategy.risk.max_position_size = 0.01
    strategy.start()
