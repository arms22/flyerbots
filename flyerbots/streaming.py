# -*- coding: utf-8 -*-
import threading
import logging
import json
import websocket
import socketio
from time import sleep
from datetime import datetime
from .utils import dotdict, stop_watch
from itertools import chain
from collections import deque, defaultdict
from functools import partial

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

def parse_order_ref_id(order_ref_id):
    return datetime(
        int(order_ref_id[3:7]),
        int(order_ref_id[7:9]),
        int(order_ref_id[9:11]),
        int(order_ref_id[12:14]),
        int(order_ref_id[14:16]),
        int(order_ref_id[16:18]),
        int(order_ref_id[19:]))

def lightning_channels(product_id, topics):
    return ['lightning_' + t + '_' + product_id.replace('/','_') for t in topics]

def lightning_channel(product_id, topic):
    return 'lightning_' + topic + '_' + product_id.replace('/','_')

class Streaming:

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.ws = None
        self.sio = None
        self.running = False
        self.subscribed_channels = []
        self.endpoints = []
        self.connected = False
        self.callbacks = defaultdict(list)

    def ws_on_message(self, message):
        message = json.loads(message)
        if message["method"] == "channelMessage":
            channel = message["params"]["channel"]
            message = message["params"]["message"]
            self.on_data(channel,message)

    def ws_on_error(self, error):
        self.logger.info(error)

    def ws_on_close(self):
        self.logger.info('disconnected')
        self.connected = False

    def ws_on_open(self):
        self.logger.info('connected')
        self.connected = True
        if len(self.subscribed_channels):
            for channel in self.subscribed_channels:
                self.ws_subscribe(channel)

    def ws_subscribe(self,channel):
        self.ws.send(json.dumps({'method': 'subscribe', 'params': {'channel': channel}}))

    def ws_run_loop(self):
        while self.running:
            try:
                self.ws = websocket.WebSocketApp("wss://ws.lightstream.bitflyer.com/json-rpc",
                    on_message=self.ws_on_message,
                    on_error=self.ws_on_error,
                    on_close=self.ws_on_close)
                self.ws.on_open = self.ws_on_open
                self.ws.run_forever()
            except Exception as e:
                self.logger.exception(e)
            if self.running:
                sleep(5)

    def sio_on_data(self, channel, data):
        self.on_data(channel,data)

    def sio_on_disconnect(self):
        self.logger.info('disconnected')
        self.connected = False

    def sio_on_connect(self):
        self.logger.info('connected')
        self.connected = True
        if len(self.subscribed_channels):
            for channel in self.subscribed_channels:
                self.sio_subscribe(channel)

    def sio_subscribe(self,channel):
        self.sio.on(channel,partial(self.sio_on_data,channel))
        self.sio.emit('subscribe',channel)

    def sio_run_loop(self):
        while self.running:
            try:
                self.sio = socketio.Client(reconnection=True, reconnection_attempts=0, reconnection_delay=1, reconnection_delay_max=30)
                self.sio.on('connect', self.sio_on_connect)
                self.sio.on('disconnect', self.sio_on_disconnect)
                self.sio.connect('https://io.lightstream.bitflyer.com', transports = ['websocket'])
                self.sio.wait()
            except Exception as e:
                self.logger.exception(e)
            if self.running:
                sleep(5)

    def on_data(self,channel,data):
        for cb in self.callbacks[channel]:
            cb(channel,data)

    def get_endpoint(self, product_id='FX_BTC_JPY', topics=['ticker', 'executions']):
        ep = self.get_endpoint_for_channels(lightning_channels(product_id,topics))
        ep.product_id = product_id
        return ep

    def get_endpoint_for_channels(self, channels):
        ep = Streaming.Endpoint(self.logger)
        self.endpoints.append(ep)
        for channel in channels:
            self.subscribe_channel(channel,ep.put)
        return ep

    def subscribe_channel(self, channel, callback):
        self.callbacks[channel].append(callback)
        if channel not in self.subscribed_channels:
            if self.connected:
                self.subscribe(channel)
            self.subscribed_channels.append(channel)

    def start(self):
        self.logger.info('Start Streaming')
        self.running = True
        self.subscribe = self.sio_subscribe
        self.thread = threading.Thread(target=self.sio_run_loop)
        self.thread.start()

    def stop(self):
        if self.running:
            self.logger.info('Stop Streaming')
            self.running = False
            self.sio.disconnect()
            self.thread.join()
            for ep in self.endpoints:
                ep.shutdown()

    class Endpoint:

        def __init__(self, logger):
            self.logger = logger
            self.cond = threading.Condition()
            self.data = defaultdict(lambda:deque(maxlen=1000))
            self.latest = defaultdict(lambda:None)
            self.closed = False
            self.suspend_count = 0
            self.product_id = ''

        def put(self, channel, message):
            with self.cond:
                self.data[channel].append(message)
                self.latest[channel] = message
                self.cond.notify_all()

        def suspend(self, flag):
            with self.cond:
                if flag:
                    self.suspend_count += 1
                else:
                    self.suspend_count = max(self.suspend_count-1, 0)
                self.cond.notify_all()

        def wait_for(self, topics=[], product_id=None):
            channels = lightning_channels(product_id or self.product_id, topics)
            for channel in channels:
                while True:
                    data = self.data[channel]
                    if len(data) or self.closed:
                        break
                    else:
                        self.logger.info('Waiting for stream data...')
                        sleep(1)

        def wait_any(self, topics=[], timeout=None, product_id=None):
            channels = lightning_channels(product_id or self.product_id, topics)
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
                        if len(channels)==0:
                            break
            return result

        def shutdown(self):
            with self.cond:
                self.closed = True
                self.cond.notify_all()

        def get_channel_data(self, channel, blocking, timeout):
            with self.cond:
                if blocking:
                    while True:
                        if len(self.data[channel]) or self.closed:
                            break
                        else:
                            if self.cond.wait(timeout) == False:
                                break
                data = list(self.data[channel])
                self.data[channel].clear()
            return data

        def get_ticker(self, blocking=False, timeout=None, product_id=None):
            channel = lightning_channel(product_id or self.product_id, 'ticker')
            self.get_channel_data(channel, blocking, timeout)
            return self.latest[channel]

        def get_tickers(self,blocking=False, timeout=None, product_id=None):
            channel = lightning_channel(product_id or self.product_id, 'ticker')
            return self.get_channel_data(channel, blocking, timeout)

        def get_executions(self, blocking=False, timeout=None, product_id=None, chained=True):
            channel = lightning_channel(product_id or self.product_id, 'executions')
            data = self.get_channel_data(channel, blocking, timeout)
            if not chained:
                return data
            return list(chain.from_iterable(data))

        def get_board_snapshot(self, blocking=False, timeout=None, product_id=None):
            channel = lightning_channel(product_id or self.product_id, 'board_snapshot')
            self.get_channel_data(channel, blocking, timeout)
            return self.latest[channel]

        def get_boards(self, blocking=False, timeout=None, product_id=None):
            channel = lightning_channel(product_id or self.product_id, 'board')
            return self.get_channel_data(channel, blocking, timeout)


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
