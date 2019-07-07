# -*- coding: utf-8 -*-
from functools import wraps
from time import sleep, time
from datetime import datetime, timedelta
import concurrent.futures
import threading
import ccxt
import logging
import json
from .utils import dotdict, stop_watch
from .order import OrderManager
from .webapi2 import LightningAPI, LightningError
from collections import OrderedDict, deque
from math import fsum

class Exchange:

    def __init__(self, apiKey = '', secret = ''):
        self.apiKey = apiKey
        self.secret = secret
        self.logger = logging.getLogger(__name__)
        self.responce_times = deque(maxlen=3)
        self.lightning_enabled = False
        self.lightning_collateral = None
        self.order_is_not_accepted = None
        self.ltp = 0
        self.last_position_size = 0
        self.api_token_cond = threading.Condition()
        self.api_token = self.max_api_token = 10

    def get_api_token(self):
        with self.api_token_cond:
            while self.running:
                if self.api_token>0:
                    self.api_token -= 1
                    break
                self.logger.info("API rate limit exceeded")
                if not self.api_token_cond.wait(timeout=60): # フェールセーフ 60秒でタイムアウト
                    self.logger.info("get_api_token() timeout")
                    break

    def feed_api_token(self):
        # 3秒毎にトークンを5つ追加する(5分500リクエスト)
        while self.running:
            try:
                with self.api_token_cond:
                    self.api_token = min(self.api_token+5,self.max_api_token)
                    self.api_token_cond.notify_all()
            except Exception as e:
                self.logger.warning(type(e).__name__ + ": {0}".format(e))
            sleep(3)

    def measure_response_time(self, func):
        @wraps(func)
        def wrapper(*args, **kargs):
            retry = 3
            while retry > 0:
                retry = retry - 1
                try:
                    start = time()
                    result = func(*args,**kargs)
                    responce_time = (time() - start)
                    self.responce_times.append(responce_time)
                    return result
                except ccxt.ExchangeError as e:
                    if (retry == 0) or ('Connection reset by peer' not in e.args[0]):
                        raise e
                sleep(0.3)
        return wrapper

    def api_state(self):
        res_times = list(self.responce_times)
        mean_time = sum(res_times) / len(res_times)
        health = 'super busy'
        if mean_time < 0.2:
            health = 'normal'
        elif mean_time < 0.5:
            health = 'busy'
        elif mean_time < 1.0:
            health = 'very busy'
        return health, mean_time, self.api_token

    def start(self):
        self.logger.info('Start Exchange')
        self.running = True

        # 取引所セットアップ
        self.exchange = ccxt.bitflyer({'apiKey':self.apiKey,'secret':self.secret})
        self.exchange.urls['api'] = 'https://api.bitflyer.com'
        self.exchange.timeout = 60 * 1000

        # レートリミット制御を上書き
        self.exchange.enableRateLimit = True
        self.exchange.throttle = self.get_api_token

        # 応答時間計測用にラッパーをかぶせる
        self.exchange.fetch = self.measure_response_time(self.exchange.fetch)

        # RestAPIとWebAPI切り換え用
        self.inter_create_order = self.__restapi_create_order
        self.inter_cancel_order = self.__restapi_cancel_order
        self.inter_cancel_order_all = self.__restapi_cancel_order_all
        self.inter_fetch_collateral = self.__restapi_fetch_collateral
        self.inter_fetch_position = self.__restapi_fetch_position
        self.inter_fetch_balance = self.__restapi_fetch_balance
        self.inter_fetch_orders = self.__restapi_fetch_orders
        self.inter_fetch_board_state = self.__restapi_fetch_board_state
        self.inter_check_order_status = self.__restapi_check_order_status

        # プライベートAPI有効判定
        self.private_api_enabled = len(self.apiKey)>0 and len(self.secret)>0

        # スレッドプール作成
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=9)

        # APIトークンフィーダー起動
        self.executor.submit(self.feed_api_token)

        # 並列注文処理完了待ち用リスト
        self.parallel_orders = []

        # 注文管理
        self.om = OrderManager()

        # Lightningログイン
        if self.lightning_enabled:
            self.lightning.login()
            # LightningAPIに置き換える
            self.inter_create_order = self.__lightning_create_order
            # self.inter_cancel_order = self.__lightning_cancel_order
            self.inter_cancel_order_all = self.__lightning_cancel_order_all
            self.inter_fetch_position = self.__lightning_fetch_position_and_collateral
            self.inter_fetch_balance = self.__lightning_fetch_balance
            # self.inter_fetch_orders = self.__lightning_fetch_orders
            # self.inter_fetch_board_state = self.__lightning_fetch_board_state
            # self.inter_check_order_status = self.__lightning_check_order_status

        # マーケット一覧表示
        self.exchange.load_markets()
        for k, v in self.exchange.markets.items():
            self.logger.info('Markets: ' + v['symbol'])

    def stop(self):
        if self.running:
            self.logger.info('Stop Exchange')
            self.running = False
            # すべてのワーカスレッド停止
            self.executor.shutdown()
            # Lightningログオフ
            if self.lightning_enabled:
                self.lightning.logoff()

    def get_order(self, myid):
        return self.om.get_order(myid)

    def get_open_orders(self):
        orders = self.om.get_orders(status_filter = ['open', 'accepted'])
        orders_by_myid = OrderedDict()
        for o in orders.values():
            orders_by_myid[o['myid']] = o
        return orders_by_myid

    def create_order(self, myid, side, qty, limit, stop, time_in_force, minute_to_expire, symbol):
        """新規注文"""
        if self.private_api_enabled:
            self.parallel_orders.append(self.executor.submit(self.inter_create_order,
                                myid, side, qty,limit, stop,time_in_force, minute_to_expire, symbol))

    def cancel(self, myid):
        """注文をキャンセルする"""
        if self.private_api_enabled:
            cancel_orders = self.om.cancel_order(myid)
            for o in cancel_orders:
                self.parallel_orders.append(self.executor.submit(self.inter_cancel_order, o))

    def cancel_open_orders(self, symbol):
        """すべての注文をキャンセルする"""
        if self.private_api_enabled:
            cancel_orders = self.om.cancel_order_all()
            for o in cancel_orders:
                self.parallel_orders.append(self.executor.submit(self.inter_cancel_order, o))

    def cancel_order_all(self, symbol):
        """すべての注文をキャンセルする"""
        if self.private_api_enabled:
            cancel_orders = self.om.cancel_order_all()
            if len(cancel_orders):
                self.inter_cancel_order_all(symbol=symbol)

    def __restapi_cancel_order_all(self, symbol):
        self.exchange.private_post_cancelallchildorders(
            params={'product_code': self.exchange.market_id(symbol)})

    def __restapi_cancel_order(self, order):
        self.exchange.cancel_order(order['id'], order['symbol'])
        self.logger.info("CANCEL: {myid} {status} {side} {price} {filled}/{amount} {id}".format(**order))

    def __restapi_create_order(self, myid, side, qty, limit, stop, time_in_force, minute_to_expire, symbol):
        # raise ccxt.ExchangeNotAvailable('sendchildorder {"status":-208,"error_message":"Order is not accepted"}')
        qty = round(qty,8) # 有効桁数8桁
        order_type = 'market'
        params = {}
        if limit is not None:
            order_type = 'limit'
            limit = float(limit)
        if time_in_force is not None:
            params['time_in_force'] = time_in_force
        if minute_to_expire is not None:
            params['minute_to_expire'] = minute_to_expire
        order = dotdict(self.exchange.create_order(symbol, order_type, side, qty, limit, params))
        order.myid = myid
        order.accepted_at = datetime.utcnow()
        order = self.om.add_order(order)
        self.logger.info("NEW: {myid} {status} {side} {price} {filled}/{amount} {id}".format(**order))

    def __restapi_fetch_position(self, symbol):
        #raise ccxt.ExchangeError("ConnectionResetError(104, 'Connection reset by peer')")
        position = dotdict()
        position.currentQty = 0
        position.avgCostPrice = 0
        position.unrealisedPnl = 0
        position.all = []
        if self.private_api_enabled:
            res = self.exchange.private_get_getpositions(
                        params={'product_code': self.exchange.market_id(symbol)})
            position.all = res
            for r in res:
                size = r['size'] if r['side'] == 'BUY' else r['size'] * -1
                cost = (position.avgCostPrice * abs(position.currentQty) + r['price'] * abs(size))
                position.currentQty = round(position.currentQty + size,8)
                position.avgCostPrice = int(cost / abs(position.currentQty))
                position.unrealisedPnl = position.unrealisedPnl + r['pnl']
                self.logger.info('{side} {price} {size} ({pnl})'.format(**r))
            self.logger.info("POSITION: qty {currentQty} cost {avgCostPrice:.0f} pnl {unrealisedPnl}".format(**position))
        return position

    def fetch_position(self, symbol, async = True):
        """建玉一覧取得"""
        if async:
            return self.executor.submit(self.inter_fetch_position, symbol)
        return self.inter_fetch_position(symbol)

    def __restapi_fetch_collateral(self):
        collateral = dotdict()
        collateral.collateral = 0
        collateral.open_position_pnl = 0
        collateral.require_collateral = 0
        collateral.keep_rate = 0
        if self.private_api_enabled:
            collateral = dotdict(self.exchange.private_get_getcollateral())
            # self.logger.info("COLLATERAL: {collateral} open {open_position_pnl} require {require_collateral:.2f} rate {keep_rate}".format(**collateral))
        return collateral

    def fetch_collateral(self, async = True):
        """証拠金情報を取得"""
        if async:
            return self.executor.submit(self.inter_fetch_collateral)
        return self.inter_fetch_collateral()

    def __restapi_fetch_balance(self):
        balance = dotdict()
        if self.private_api_enabled:
            res = self.exchange.private_get_getbalance()
            for v in res:
                balance[v['currency_code']] = dotdict(v)
        return balance

    def fetch_balance(self, async = True):
        """資産情報取得"""
        if async:
            return self.executor.submit(self.inter_fetch_balance)
        return self.inter_fetch_balance()

    def fetch_open_orders(self, symbol, limit=100):
        orders = []
        if self.private_api_enabled:
            orders = self.exchange.fetch_open_orders(symbol=symbol, limit=limit)
            # for order in orders:
            #     self.logger.info("{side} {price} {amount} {status} {id}".format(**order))
        return orders

    def __restapi_fetch_orders(self, symbol, limit):
        orders = []
        if self.private_api_enabled:
            orders = self.exchange.fetch_orders(symbol=symbol,limit=limit)
            # for order in orders:
            #     self.logger.info("{side} {price} {amount} {status} {id}".format(**order))
        return orders

    def fetch_orders(self, symbol, limit=100, async=False):
        if async:
            return self.executor.submit(self.inter_fetch_orders, symbol, limit)
        return self.inter_fetch_orders(symbol, limit)

    def fetch_order_book(self, symbol):
        """板情報取得"""
        return dotdict(self.exchange.public_get_getboard(
                    params={'product_code': self.exchange.market_id(symbol)}))

    def wait_for_completion(self):

        # 新規注文拒否解除
        if self.order_is_not_accepted:
            past = datetime.utcnow() - self.order_is_not_accepted
            if past > timedelta(seconds=3):
                self.order_is_not_accepted = None

        # 注文完了確認
        for f in concurrent.futures.as_completed(self.parallel_orders):
            try:
                res = {}
                try:
                    f.result()
                except ccxt.ExchangeNotAvailable as e:
                    self.logger.warning(type(e).__name__ + ": {0}".format(e))
                    msg = e.args[0]
                    if '{' in msg:
                        res = json.loads(msg[msg.find('{'):])
                except ccxt.DDoSProtection as e:
                    self.logger.warning(type(e).__name__ + ": {0}".format(e))
                    self.order_is_not_accepted = datetime.utcnow()+timedelta(seconds=12)
                except LightningError as e:
                    self.logger.warning(type(e).__name__ + ": {0}".format(e))
                    res = e.args[0]
                # 注文を受付けることができませんでした.
                if 'status' in res and res['status'] == -208:
                    self.order_is_not_accepted = datetime.utcnow()
            except Exception as e:
                self.logger.warning(type(e).__name__ + ": {0}".format(e))
        self.parallel_orders = []

    def get_position(self):
        size = 0
        avg = 0
        pnl = 0
        with self.om.lock:
            positions = list(self.om.positions)
        if len(positions):
            size = fsum(p['size'] for p in positions)
            avg = fsum(p['price']*p['size'] for p in positions)/size
            size = size if positions[0]['side'] == 'buy' else size*-1
            pnl = (self.ltp * size) - (avg * size)
        if self.last_position_size != size:
            # self.logger.info("POSITION: qty {0:.8f} cost {1:.0f} pnl {2:.8f}".format(size, avg, pnl))
            self.last_position_size = size
        return size, avg, pnl, positions

    def restore_position(self, positions):
        with self.om.lock:
            self.om.positions = deque()
            for p in positions:
                self.om.positions.append({'side':p['side'].lower(), 'size':p['size'], 'price':p['price']})

    def order_exec(self, o, e):
        if o is not None:
            if self.om.execute(o,e):
                self.logger.info("EXEC: {myid} {status} {side} {price} {filled}/{amount} {average_price} {id}".format(**o))

    def check_order_execution(self, executions):
        if len(executions):
            self.ltp = executions[-1]['price']
            my_orders = self.om.get_orders(status_filter = ['open', 'accepted', 'cancel','canceled'])
            if len(my_orders):
                for e in executions:
                    o = my_orders.get(e['buy_child_order_acceptance_id'], None)
                    self.order_exec(o,e)
                    o = my_orders.get(e['sell_child_order_acceptance_id'], None)
                    self.order_exec(o,e)

    def check_order_open_and_cancel(self, boards):
        if len(boards):
            my_orders = self.om.get_open_orders()
            if len(my_orders):
                for board in boards:
                    bids = {b['price']:b['size'] for b in board['bids']}
                    asks = {b['price']:b['size'] for b in board['asks']}
                    for o in my_orders.values():
                        size = None
                        if o['side'] == 'buy':
                            size = bids.get(o['price'], None)
                        elif o['side'] == 'sell':
                            size = asks.get(o['price'], None)
                        if size is not None:
                            if self.om.open_or_cancel(o, size):
                                self.logger.info("UPDATE: {myid} {status} {side} {price} {filled}/{amount} {average_price} {id}".format(**o))

    def start_monitoring(self, endpoint):

        def monitoring_main(ep):
            self.logger.info('Start Monitoring')
            while self.running and not ep.closed:
                try:
                    ep.wait_any()
                    executions = ep.get_executions()
                    self.check_order_execution(executions)
                    boards = ep.get_boards()
                    self.check_order_open_and_cancel(boards)
                except Exception as e:
                    self.logger.warning(type(e).__name__ + ": {0}".format(e))
            self.logger.info('Stop Monitoring')

        self.executor.submit(monitoring_main, endpoint)

    def __restapi_check_order_status(self, show_last_n_orders = 0):
        my_orders = self.om.get_open_orders()
        if len(my_orders):
            # マーケット毎の注文一覧を取得する
            symbols = list(set(v['symbol'] for v in my_orders.values()))
            latest_orders = []
            for symbol in symbols:
                latest_orders.extend(self.exchange.fetch_orders(symbol=symbol, limit=50))
            # 注文情報更新
            for latest in latest_orders:
                o = my_orders.get(latest['id'], None)
                if o is not None:
                    # 最新の情報で上書き
                    self.om.overwrite(o, latest)
                    # self.logger.info("STATUS: {myid} {status} {side} {price} {filled}/{amount} {id}".format(**o))
                    del my_orders[latest['id']]
            # 注文一覧から消えた注文はcanceledとする
            for o in my_orders.values():
                self.om.expire(o)
        # 直近n個の注文を表示
        if show_last_n_orders:
            my_orders = list(self.om.get_orders().values())
            if len(my_orders) > show_last_n_orders:
                my_orders = my_orders[-show_last_n_orders:]
            self.logger.info('No    myid     status   side    price amount filled average_price')
            for o in my_orders:
                self.logger.info('{No:<5} {myid:<8} {status:<8} {side:<4} {price:>8.0f} {amount:6.2f} {filled:6.2f} {average_price:>13.0f} {type} {symbol} {id}'.format(**o))

        # 注文情報整理
        remaining_orders = max(show_last_n_orders, 30)
        self.om.cleaning_if_needed(limit_orders=remaining_orders*2, remaining_orders=remaining_orders)

    def check_order_status(self, show_last_n_orders = 0, async = True):
        """注文の状態を確認"""
        if async:
            return self.executor.submit(self.inter_check_order_status, show_last_n_orders)
        self.inter_check_order_status(show_last_n_orders)

    def __restapi_fetch_board_state(self, symbol):
        res = dotdict(self.exchange.public_get_getboardstate(
            params={'product_code': self.exchange.market_id(symbol)}))
        self.logger.info("health {health} state {state}".format(**res))
        return res

    def fetch_board_state(self, symbol, async = True):
        """板状態取得"""
        if async:
            return self.executor.submit(self.inter_fetch_board_state, symbol)
        return self.inter_fetch_board_state(symbol)

    def enable_lightning_api(self, userid, password):
        """LightningAPIを有効にする"""
        self.lightning = LightningAPI(userid, password)
        self.lightning_enabled = True

    def __lightning_create_order(self, myid, side, qty, limit, stop, time_in_force, minute_to_expire, symbol):
        # raise LightningError({'status':-208})
        qty = round(qty,8) # 有効桁数8桁
        ord_type = 'MARKET'
        if limit is not None:
            ord_type = 'LIMIT'
            limit = int(limit)
        res = self.lightning.sendorder(self.exchange.market_id(symbol), ord_type, side.upper(), limit, qty, minute_to_expire, time_in_force)
        order = dotdict()
        order.myid = myid
        order.accepted_at = datetime.utcnow()
        order.id = res['order_ref_id']
        order.status = 'accepted'
        order.symbol = symbol
        order.type = ord_type.lower()
        order.side = side
        order.price = limit if limit is not None else 0
        order.average_price = 0
        order.cost = 0
        order.amount = qty
        order.filled = 0
        order.remaining = 0
        order.fee = 0
        order = self.om.add_order(order)
        self.logger.info("NEW: {myid} {status} {side} {price} {filled}/{amount} {id}".format(**order))

    def __lightning_cancel_order(self, order):
        self.lightning.cancelorder(product_code=self.exchange.market_id(order['symbol']), order_id=order['id'])
        self.logger.info("CANCEL: {myid} {status} {side} {price} {filled}/{amount} {id}".format(**order))

    def __lightning_cancel_order_all(self, symbol):
        self.lightning.cancelallorder(product_code=self.exchange.market_id(symbol))

    def __lightning_fetch_position_and_collateral(self, symbol):
        position = dotdict()
        position.currentQty = 0
        position.avgCostPrice = 0
        position.unrealisedPnl = 0
        collateral = dotdict()
        collateral.collateral = 0
        collateral.open_position_pnl = 0
        collateral.require_collateral = 0
        collateral.keep_rate = 0
        if self.lightning_enabled:
            res = self.lightning.getmyCollateral(product_code=self.exchange.market_id(symbol))
            collateral.collateral = res['collateral']
            collateral.open_position_pnl = res['open_position_pnl']
            collateral.require_collateral = res['require_collateral']
            collateral.keep_rate = res['keep_rate']
            position.all = res['positions']
            for r in position.all:
                size = r['size'] if r['side'] == 'BUY' else r['size'] * -1
                cost = (position.avgCostPrice * abs(position.currentQty) + r['price'] * abs(size))
                position.currentQty = round(position.currentQty + size,8)
                position.avgCostPrice = cost / abs(position.currentQty)
                position.unrealisedPnl = position.unrealisedPnl + r['pnl']
                self.logger.info('{side} {price} {size} ({pnl})'.format(**r))
            self.logger.info("POSITION: qty {currentQty} cost {avgCostPrice:.0f} pnl {unrealisedPnl}".format(**position))
            self.logger.info("COLLATERAL: {collateral} open {open_position_pnl} require {require_collateral:.2f} rate {keep_rate}".format(**collateral))
        return position, collateral

    def __lightning_fetch_balance(self):
        balance = dotdict()
        if self.lightning_enabled:
            res = self.lightning.inventories()
            for k, v in res.items():
                balance[k] = dotdict(v)
        return balance
