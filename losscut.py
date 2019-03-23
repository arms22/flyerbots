# -*- coding: utf-8 -*-
from flyerbots.strategy import Strategy
from flyerbots.indicator import *


class LossCut:

    def __init__(self):
        pass

    def loop(self, ticker, strategy, **other):
        bid = ticker.best_bid
        ask = ticker.best_ask
        spr = ask - bid
        bid_20pct = int(bid * (1-0.1999))
        ask_20pct = int(ask * (1+0.1999))

        logger.info('buy/sell {bid_20pct}/{ask_20pct} bid/ask {bid}/{ask}({spr})'.format(**locals()))

        qty_lot = 1
        strategy.entry('L', 'buy', qty=qty_lot, limit=bid_20pct, minute_to_expire=1)
        strategy.entry('S', 'sell', qty=qty_lot, limit=ask_20pct, minute_to_expire=1)


if __name__ == "__main__":
    import settings
    import logging
    import logging.config

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("LossCut")

    strategy = Strategy(LossCut().loop, 60)
    strategy.settings.symbol = 'BTCJPY28DEC2018'
    strategy.settings.apiKey = settings.apiKey
    strategy.settings.secret = settings.secret
    strategy.risk.max_position_size = 1
    strategy.start()
