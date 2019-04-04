# -*- coding: utf-8 -*-
import threading
import logging
import json
import websocket
from datetime import datetime
from time import sleep
import pandas as pd
from .utils import dotdict, stop_watch
import math
from itertools import chain
from collections import deque

class Streaming:

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.ws = None
        self.running = False
        self.subscribed_channels = []
        self.endpoints = []
        self.connected = False
        # self.on_message = stop_watch(self.on_message)

    def on_message(self, message):
        message = json.loads(message)
        if message["method"] == "channelMessage":
            channel = message["params"]["channel"]
            message = message["params"]["message"]
            for ep in self.endpoints:
                ep.put(channel, message)

    def on_error(self, error):
        self.logger.info(error)

    def on_close(self):
        self.logger.info('disconnected')
        self.connected = False

    def on_open(self):
        self.logger.info('connected')
        self.connected = True
        if len(self.subscribed_channels):
            for channel in self.subscribed_channels:
                self.ws.send(json.dumps({'method': 'subscribe', 'params': {'channel': channel}}))

    def get_endpoint(self, product_id='FX_BTC_JPY', topics=['ticker', 'executions'], timeframe=60, max_ohlcv_size=100):
        ep = Streaming.Endpoint(product_id, topics, self.logger, timeframe, max_ohlcv_size)
        self.endpoints.append(ep)
        for channel in ep.channels:
            if channel not in self.subscribed_channels:
                if self.connected:
                    self.ws.send(json.dumps({'method': 'subscribe', 'params': {'channel': channel}}))
                self.subscribed_channels.append(channel)
        return ep

    def ws_run_loop(self):
        while self.running:
            try:
                self.ws = websocket.WebSocketApp("wss://ws.lightstream.bitflyer.com/json-rpc",
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close)
                self.ws.on_open = self.on_open
                self.ws.run_forever()
            except Exception as e:
                self.logger.exception(e)
            if self.running:
                sleep(5)

    def start(self):
        self.logger.info('Start Streaming')
        self.running = True
        self.thread = threading.Thread(target=self.ws_run_loop)
        self.thread.start()

    def stop(self):
        if self.running:
            self.logger.info('Stop Streaming')
            self.running = False
            self.ws.close()
            self.thread.join()
            for ep in self.endpoints:
                ep.shutdown()

    class Endpoint:

        def __init__(self, product_id, topics, logger, timeframe, max_ohlcv_size):
            self.logger = logger
            self.product_id = product_id.replace('/','_')
            self.topics = topics
            self.cond = threading.Condition()
            self.channels = ['lightning_' + t + '_' + self.product_id for t in topics]
            self.data = {}
            self.last = {}
            self.closed = False
            self.suspend_count = 0
            for channel in self.channels:
                self.data[channel] = deque(maxlen=10000)
                self.last[channel] = None
            # self.get_data = stop_watch(self.get_data)
            # self.put = stop_watch(self.put)
            # self.parse_exec_date = stop_watch(self.parse_exec_date)
            # self.make_ohlcv = stop_watch(self.make_ohlcv)
            # self.create_ohlcv = stop_watch(self.create_ohlcv)
            # self.get_lazy_ohlcv = stop_watch(self.get_lazy_ohlcv)
            # self.get_boundary_ohlcv = stop_watch(self.get_boundary_ohlcv)
            self.timeframe = timeframe
            self.ohlcv = deque(maxlen=max_ohlcv_size)
            self.remain_executions = []
            self.lst_timeframe = datetime.utcnow().timestamp() // timeframe

        def put(self, channel, message):
            if channel in self.data:
                with self.cond:
                    self.data[channel].append(message)
                    self.last[channel] = message
                    self.cond.notify_all()

        def suspend(self, flag):
            with self.cond:
                if flag:
                    self.suspend_count += 1
                else:
                    self.suspend_count = max(self.suspend_count-1, 0)
                self.cond.notify_all()

        def wait_for(self, topics = None):
            topics = topics or self.topics
            for topic in topics:
                channel = 'lightning_' + topic + '_' + self.product_id
                while True:
                    data = self.data[channel]
                    if len(data) or self.closed:
                        break
                    else:
                        self.logger.info('Waiting for stream data...')
                        sleep(1)

        def wait_any(self, topics = None, timeout = None):
            topics = topics or self.topics
            channels = ['lightning_' + t + '_' + self.product_id for t in topics]
            result = True
            with self.cond:
                while True:
                    available = 0
                    if self.suspend_count == 0:
                        for channel in channels:
                            available = available + len(self.data[channel])
                    if available or self.closed:
                        break
                    else:
                        if self.cond.wait(timeout) == False:
                            result = False
                            break
            return result

        def shutdown(self):
            with self.cond:
                self.closed = True
                self.cond.notify_all()

        def get_data(self, topic, blocking, timeout):
            channel = 'lightning_' + topic + '_' + self.product_id
            if channel in self.data:
                with self.cond:
                    if blocking:
                        while True:
                            if len(self.data[channel]) or self.closed:
                                break
                            else:
                                if self.cond.wait(timeout) == False:
                                    break
                    data = list(self.data[channel])
                    last = self.last[channel]
                    self.data[channel].clear()
                return data, last
            return [], None

        def get_last(self, topic):
            channel = 'lightning_' + topic + '_' + self.product_id
            return self.last[channel] if channel in self.data else None

        def get_ticker(self, blocking = False, timeout = None):
            data, last = self.get_data('ticker', blocking, timeout)
            return last

        def get_tickers(self, blocking = False, timeout = None):
            data, last = self.get_data('ticker', blocking, timeout)
            return data

        def get_executions(self, blocking = False, timeout = None):
            data, last = self.get_data('executions', blocking, timeout)
            return list(chain.from_iterable(data))

        def get_board_snapshot(self, blocking = False, timeout = None):
            data, last = self.get_data('board_snapshot', blocking, timeout)
            return last

        def get_boards(self, blocking = False, timeout = None):
            data, last = self.get_data('board', blocking, timeout)
            return data

        @staticmethod
        def parse_exec_date(exec_date):
            exec_date = exec_date.rstrip('Z')+'0000000'
            return datetime(
                int(exec_date[0:4]),
                int(exec_date[5:7]),
                int(exec_date[8:10]),
                int(exec_date[11:13]),
                int(exec_date[14:16]),
                int(exec_date[17:19]),
                int(exec_date[20:26]))

        @staticmethod
        def parse_order_ref_id(order_ref_id):
            return datetime(
                int(order_ref_id[3:7]),
                int(order_ref_id[7:9]),
                int(order_ref_id[9:11]),
                int(order_ref_id[12:14]),
                int(order_ref_id[14:16]),
                int(order_ref_id[16:18]),
                int(order_ref_id[19:]))

        def get_lazy_ohlcv(self):
            data, last = self.get_data('executions',False,None)
            if len(self.remain_executions)>0:
                self.ohlcv.pop()
            if len(data)==0:
                e = last[-1].copy()
                e['size'] = 0
                e['side'] = ''
                data.append([e])
            for dat in data:
                closed_at = self.parse_exec_date(dat[-1]['exec_date'])
                cur_timeframe = closed_at.timestamp() // self.timeframe
                if cur_timeframe > self.lst_timeframe:
                    if len(self.remain_executions) > 0:
                        self.ohlcv.append(self.make_ohlcv(self.remain_executions))
                    self.remain_executions = []
                    self.lst_timeframe = cur_timeframe
                self.remain_executions.extend(dat)
            if len(self.remain_executions) > 0:
                self.ohlcv.append(self.make_ohlcv(self.remain_executions))
            return list(self.ohlcv)

        def get_boundary_ohlcv(self):
            data, last = self.get_data('executions',False,None)
            executions = list(chain.from_iterable(data))
            if len(executions)==0:
                e = last[-1].copy()
                e['size'] = 0
                e['side'] = ''
                executions.append(e)
            self.ohlcv.append(self.make_ohlcv(executions))
            return list(self.ohlcv)

        def make_ohlcv(self, executions):
            price = [e['price'] for e in executions]
            buy = [e for e in executions if e['side'] == 'BUY']
            sell = [e for e in executions if e['side'] == 'SELL']
            ohlcv = dotdict()
            ohlcv.open = price[0]
            ohlcv.high = max(price)
            ohlcv.low = min(price)
            ohlcv.close = price[-1]
            ohlcv.volume = sum(e['size'] for e in executions)
            ohlcv.buy_volume = sum(e['size'] for e in buy)
            ohlcv.sell_volume = sum(e['size'] for e in sell)
            ohlcv.volume_imbalance = ohlcv.buy_volume - ohlcv.sell_volume
            ohlcv.buy_count = len(buy)
            ohlcv.sell_count = len(sell)
            ohlcv.trades = ohlcv.buy_count + ohlcv.sell_count
            ohlcv.imbalance = ohlcv.buy_count - ohlcv.sell_count
            ohlcv.average = sum(price) / len(price)
            ohlcv.average_sq = sum(p**2 for p in price) / len(price)
            ohlcv.variance = ohlcv.average_sq - (ohlcv.average * ohlcv.average)
            ohlcv.stdev = math.sqrt(ohlcv.variance)
            ohlcv.vwap = sum(e['price']*e['size'] for e in executions) / ohlcv.volume if ohlcv.volume > 0 else price[-1]
            ohlcv.created_at = datetime.utcnow()
            ohlcv.closed_at = self.parse_exec_date(executions[-1]['exec_date'])
            e = executions[-1]
            if e['side']=='SELL':
                ohlcv.market_order_delay = (ohlcv.closed_at-self.parse_order_ref_id(e['sell_child_order_acceptance_id'])).total_seconds()
            elif e['side']=='BUY':
                ohlcv.market_order_delay = (ohlcv.closed_at-self.parse_order_ref_id(e['buy_child_order_acceptance_id'])).total_seconds()
            else:
                ohlcv.market_order_delay = 0
            ohlcv.distribution_delay = (ohlcv.created_at - ohlcv.closed_at).total_seconds()
            return ohlcv

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--product_id", dest='product_id', type=str, default='FX_BTC_JPY')
    parser.add_argument("--topics", dest='topics', type=str, nargs='*', default=['executions','ticker'])
    args = parser.parse_args()

    streaming = Streaming()
    streaming.start()
    ep = streaming.get_endpoint(product_id=args.product_id, topics=args.topics)

    while True:
        try:
            ep.wait_any()
            executions = ep.get_executions()
            for e in executions:
                print('EXE {side} {price} {size} {exec_date}'.format(**e))
            tickers = ep.get_tickers()
            for t in tickers:
                print('TIK {ltp} {best_bid}({best_bid_size})/{best_ask}({best_ask_size}) {timestamp}'.format(**t))
            boards = ep.get_boards()
            for board in boards:
                for bid in board['bids']:
                    print('BID {price} {size}'.format(**bid))
                for ask in board['asks']:
                    print('ASK {price} {size}'.format(**ask))
        except (KeyboardInterrupt, SystemExit):
            break

    streaming.stop()
