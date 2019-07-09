# -*- coding: utf-8 -*-
from flyerbots.strategy import Strategy
from flyerbots.streaming import parse_exec_date
from datetime import datetime, timedelta
from math import ceil
from collections import deque

class SFDBot:

    def __init__(self):
        self.spot_q = deque(maxlen=20)
        self.spot_ltp_q = deque(maxlen=10)

    def setup(self, strategy):
        self.spot_ep = strategy.streaming.get_endpoint('BTC_JPY',['executions'])

    def loop(self, executions, strategy, **other):

        spot_executions = self.spot_ep.get_executions()
        if len(spot_executions):
            e = spot_executions[-1]
            e['exec_date'] = parse_exec_date(e['exec_date'])
            self.spot_q.append(e)
        if len(self.spot_q)<2:
            return

        dt = datetime.utcnow()
        spot_available = len(spot_executions)
        spot = self.spot_q[-1]
        spot2 = self.spot_q[-2]
        spot_ltp = spot['price']
        spot_ltp2 = spot2['price']
        spot_exec_date = spot['exec_date']
        spot_past_time = (dt - spot_exec_date).total_seconds()

        self.spot_ltp_q.append(spot_ltp)
        ltp_list = list(self.spot_ltp_q)
        ltp_uniq = set(ltp_list)
        ltp_min = min(ltp_list)
        ltp_max = max(ltp_list)

        sfdask = ceil(spot_ltp * 1.05)
        sfdbid = sfdask-1
        buymax = 0.00
        sellmax = 0.04
        lot = 0.02
        deltapos = strategy.position_size+lot

        if spot_available:
            if spot_ltp > spot_ltp2:
                strategy.cancel('S')
            elif spot_ltp < spot_ltp2:
                strategy.cancel('L')

        if strategy.api_token >= 4:
            if spot_past_time>0.37 and len(ltp_uniq)<=2:
                if deltapos+lot<=buymax:
                    strategy.order('L', 'buy', qty=lot, limit=sfdbid, minute_to_expire=1)
                if deltapos-lot>=-sellmax:
                    strategy.order('S', 'sell', qty=lot, limit=sfdask, minute_to_expire=1)

        # logger.info(f'{spot_ltp} {spot_past_time:6.3f} {ltp_min} {ltp_max}')

if __name__ == "__main__":
    import settings
    import argparse
    import logging
    import logging.config

    logging.basicConfig(level=logging.INFO)
    logging.getLogger("socketio").setLevel(logging.WARNING)
    logging.getLogger("engineio").setLevel(logging.WARNING)
    logger = logging.getLogger("SFDBot")

    sfd = SFDBot()
    strategy = Strategy(sfd.loop, 0.02, sfd.setup)
    strategy.settings.apiKey = settings.apiKey
    strategy.settings.secret = settings.secret
    strategy.settings.disable_create_ohlcv = True
    strategy.risk.max_position_size = 0.1
    strategy.start()
