# -*- coding: utf-8 -*-
from flyerbots.strategy import Strategy
from flyerbots.indicator import *
from datetime import datetime, timedelta
from math import ceil, floor
from time import sleep

class SFDBot:

    def __init__(self):
        self.fx_exec_cnt = 0
        self.fx_exec_side = ''
        self.min_order_interval = timedelta(seconds=3)
        self.next_order_accept_time = datetime.now() + self.min_order_interval
        self.sell_decition_count = 0
        self.buy_decition_count = 0

    def loop(self, ticker, executions, executions_btcjpy, ticker_btcjpy, position, **other):

        cur_time = datetime.utcnow()
        fx_last_t = pd.to_datetime(ticker.timestamp)
        btc_last_t = pd.to_datetime(ticker_btcjpy.timestamp)

        fx_ltp = ticker.ltp
        btc_ltp = ticker_btcjpy.ltp
        if len(executions):
            exec_time = pd.to_datetime(executions[-1]['exec_date'])
            self.fx_exec_cnt = self.fx_exec_cnt + len(executions)
            self.fx_exec_side = executions[-1]['side']
            if exec_time > fx_last_t:
                fx_last_t = exec_time
                fx_ltp = executions[-1]['price']

        if len(executions_btcjpy):
            exec_time = pd.to_datetime(executions_btcjpy[-1]['exec_date'])
            if exec_time > btc_last_t:
                btc_last_t = exec_time
                btc_ltp = executions_btcjpy[-1]['price']

        fx_exec_cnt = self.fx_exec_cnt
        fx_exec_side = self.fx_exec_side

        fx_delay = cur_time - fx_last_t
        fx_delay = (fx_delay.seconds + fx_delay.microseconds / 1000000)

        btc_delay = cur_time - btc_last_t
        btc_delay = (btc_delay.seconds + btc_delay.microseconds /1000000)

        sfdpct = (fx_ltp / btc_ltp) * 100 - 100
        sfdpct_bid = (ticker.best_bid / btc_ltp) * 100 - 100
        sfdpct_ask = (ticker.best_ask / btc_ltp) * 100 - 100

        bid_btcjpy = ticker_btcjpy.best_bid
        ask_btcjpy = ticker_btcjpy.best_ask
        # sfd_price = ceil(btc_ltp * 1.05000)+1
        # sfd_safe_price = floor(min(bid_btcjpy, btc_ltp) * 1.05000)

        # logger.info('{cur_time} {fx_delay:.4f} {btc_delay:.4f}'.format(**locals()))
        # logger.info('SFD {sfdpct:.4f}({sfdpct_bid:.4f}/{sfdpct_ask:.4f}) '
        #     'FX ltp[bid/ask] {fx_ltp} {fx_exec_side:<4}({fx_exec_cnt})[{ticker[best_bid]:.0f}({ticker[best_bid_size]:6.2f})/{ticker[best_ask]:.0f}({ticker[best_ask_size]:6.2f})] '
        #     'BTC ltp[bid/ask] {btc_ltp:.0f}[{ticker_btcjpy[best_bid]:.0f}/{ticker_btcjpy[best_ask]:.0f}]'.format(**locals()))

        qty_lot = 0.01

        # if position.currentQty < 0:
        #     strategy.order('L', 'buy', qty=-position.currentQty, limit=sfd_safe_price)
        # else:
        #     if sfdpct > 4.9:
        #         strategy.order('S', 'sell', qty=qty_lot, limit=sfd_price)
        #     strategy.cancel('L')

        if sfdpct_bid > 5.0000:
            self.sell_decition_count = self.sell_decition_count + 1
        else:
            self.sell_decition_count = 0
        if sfdpct_ask < 5.0000:
            self.buy_decition_count = self.buy_decition_count + 1
        else:
            self.buy_decition_count = 0            

        showlog = False
        t = datetime.now()
        if t > self.next_order_accept_time:
            if position.currentQty < 0:
                if self.sell_decition_count > 2 and ticker.best_ask_size > 1:
                    strategy.order('L', 'buy', qty=-position.currentQty, limit=ticker.best_ask, time_in_force='FOK')
                    self.next_order_accept_time =  t + self.min_order_interval
                    showlog = True
            else:
                if self.sell_decition_count > 2 and ticker.best_bid_size > 1:
                    strategy.order('S', 'sell', qty=qty_lot, limit=ticker.best_bid, time_in_force='FOK')
                    self.next_order_accept_time =  t + self.min_order_interval
                    showlog = True

        if showlog:
            logger.info('{cur_time} {fx_delay:.4f} {btc_delay:.4f}'.format(**locals()))
            logger.info('SFD {sfdpct:.4f}({sfdpct_bid:.4f}/{sfdpct_ask:.4f}) '
                'FX ltp[bid/ask] {fx_ltp} {fx_exec_side:<4}({fx_exec_cnt})[{ticker[best_bid]:.0f}({ticker[best_bid_size]:6.2f})/{ticker[best_ask]:.0f}({ticker[best_ask_size]:6.2f})] '
                'BTC ltp[bid/ask] {btc_ltp:.0f}[{ticker_btcjpy[best_bid]:.0f}/{ticker_btcjpy[best_ask]:.0f}]'.format(**locals()))


if __name__ == "__main__":
    import settings
    import argparse
    import logging
    import logging.config

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("SFDBot")

    strategy = Strategy(SFDBot().loop, 0.0)
    strategy.settings.apiKey = settings.apiKey
    strategy.settings.secret = settings.secret
    strategy.settings.disable_ohlcv = True
    strategy.risk.max_position_size = 0.01
    strategy.start()
