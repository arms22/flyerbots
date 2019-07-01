# -*- coding: utf-8 -*-
from flyerbots.strategy import Strategy
from flyerbots.indicator import *
from datetime import datetime, timedelta
from math import ceil, floor
from time import sleep

class SFDBot:

    def __init__(self):
        self.spot_ltp = None
        self.spot_det = datetime.utcnow()
        self.margin_ltp = None
        self.margin_side = ''

    def setup(self, strategy):
        self.spot_ep = strategy.streaming.get_endpoint('BTC_JPY',['executions'])
        self.spot_ep.wait_any(['executions'])
        self.margin_ep = strategy.streaming.get_endpoint('FX_BTC_JPY',['executions'])
        self.margin_ep.wait_any(['executions'])

    def loop(self, executions, strategy, **other):

        strategy.streaming.wait_any()
        T = datetime.utcnow()

        marged_executions = []
        execs = self.margin_ep.get_executions()
        for e in execs:
            e['product_id'] = 'FX'
        marged_executions.extend(execs)

        execs = self.spot_ep.get_executions()
        for e in execs:
            e['product_id'] = 'sp'
        marged_executions.extend(execs)

        marged_executions = sorted(marged_executions,key=lambda x:x['id'])
        for e in marged_executions:
            if e['product_id']=='sp':
                self.spot_ltp = e['price']
            elif e['product_id']=='FX':
                self.margin_ltp = e['price']
                self.margin_side = e['side']

        margin = self.margin_ltp
        margin_side = self.margin_side
        spot = self.spot_ltp
        sfdpct = margin/spot
        sfdask = ceil(self.spot_ltp * 1.05)
        sfdbid = sfdask-1
        maxsize = 0.03
        lot = 0.01
        deltapos = strategy.position_size+0.04

        if deltapos<maxsize:
            if margin<sfdbid-400:
                sleep(0.1)
                strategy.order('L', 'buy', qty=lot, limit=margin, seconds_to_keep_order=2, minute_to_expire=1)
            # else:
            # 	strategy.cancel('L')
        if deltapos>-maxsize:
            if margin>=sfdask:
                sleep(0.08)
                strategy.order('S', 'sell', qty=lot, limit=margin, seconds_to_keep_order=2, minute_to_expire=1)
            # else:
            # 	strategy.cancel('S')

        logger.info(f'{sfdpct:.6f} {margin_side:4} {margin:.0f} {spot:.0f} {sfdask:.0f} {sfdbid:.0f}')

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
    strategy = Strategy(sfd.loop, 0.0001, sfd.setup)
    strategy.settings.apiKey = settings.apiKey
    strategy.settings.secret = settings.secret
    strategy.settings.disable_create_ohlcv = True
    strategy.risk.max_position_size = 0.05
    strategy.start()

    