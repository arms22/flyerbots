# -*- coding: utf-8 -*-
from functools import wraps
import concurrent.futures
from time import sleep, time
from datetime import datetime, timedelta
import ccxt
import logging
import logging.config
import pandas as pd
from .streaming import Streaming
from .exchange import Exchange
from .utils import dotdict, stop_watch
from math import fsum

class Strategy:

    def __init__(self, yourlogic, interval=60, yoursetup = None):

        # トレーディングロジック設定
        self.yourlogic = yourlogic
        self.yoursetup = yoursetup

        # 取引所情報
        self.settings = dotdict()
        self.settings.symbol = 'FX_BTC_JPY'
        self.settings.topics = ['ticker', 'executions']
        self.settings.apiKey = ''
        self.settings.secret = ''

        # LightningAPI設定
        self.settings.use_lightning = False
        self.settings.lightning_userid = ''
        self.settings.lightning_password = ''

        # 動作タイミング
        self.settings.interval = interval
        self.settings.timeframe = 60

        # OHLCV生成オプション
        self.settings.max_ohlcv_size = 1000
        self.settings.use_lazy_ohlcv = False
        self.settings.disable_create_ohlcv = False
        self.settings.disable_rich_ohlcv = False

        # その為
        self.settings.show_last_n_orders = 0
        self.settings.safe_order = True

        # リスク設定
        self.risk = dotdict()
        self.risk.max_position_size = 1.0
        self.risk.max_num_of_orders = 1

        # ログ設定
        self.logger = logging.getLogger(__name__)
        # self.create_rich_ohlcv = stop_watch(self.create_rich_ohlcv)

    def fetch_order_book(self, symbol = None):
        """板情報取得"""
        return self.exchange.fetch_order_book(symbol or self.settings.symbol)

    def cancel(self, myid):
        """注文をキャンセル"""

        # 注文情報取得
        order = self.exchange.get_order(myid)

        # 注文一覧にのるまでキャンセルは受け付けない
        if (order.status == 'accepted') and self.settings.safe_order:
            delta = datetime.utcnow() - order.accepted_at
            if delta < timedelta(seconds=30):
                if not self.hft:
                    self.logger.info("REJECT: {0} order creating...".format(myid))
                return

        self.exchange.cancel(myid)

    def cancel_order_all(self, symbol = None):
        """すべての注文をキャンセルする"""
        self.exchange.cancel_order_all(symbol or self.settings.symbol)

    def close_position(self, symbol = None):
        symbol = symbol or self.settings.symbol
        if symbol == 'FX_BTC_JPY':
            min_qty = 0.01
        else:
            min_qty = 0.001
        buysize = sellsize = 0
        # 買いポジあり
        if self.position_size > 0:
            sellsize = self.position_size
            if sellsize < min_qty:
                buysize = min_qty
                sellsize = fsum([sellsize,min_qty])
        # 売りポジあり
        elif self.position_size < 0:
            buysize = -self.position_size
            if buysize < min_qty:
                buysize = fsum([buysize,min_qty])
                sellsize = min_qty
        if sellsize:
            self.exchange.create_order('L close', 'sell', sellsize, None, None, None, None, symbol)
        if buysize:
            self.exchange.create_order('S close', 'buy', buysize, None, None, None, None, symbol)

    def order(self, myid, side, qty, limit=None, stop=None, time_in_force = None, minute_to_expire = None, symbol = None, limit_mask = 0, seconds_to_keep_order = None):
        """注文"""

        if self.exchange.order_is_not_accepted is not None:
            if not self.hft:
                self.logger.info("REJECT: {0} order is not accepted...".format(myid))
            return

        qty_total = qty
        qty_limit = self.risk.max_position_size

        # 買いポジあり
        if self.position_size > 0:
            # 買い増し
            if side == 'buy':
                # 現在のポジ数を加算
                qty_total = qty_total + self.position_size
            else:
                # 反対売買の場合、ドテンできるように上限を引き上げる
                qty_limit = qty_limit + self.position_size

        # 売りポジあり
        if self.position_size < 0:
            # 売りまし
            if side == 'sell':
                # 現在のポジ数を加算
                qty_total = qty_total + -self.position_size
            else:
                # 反対売買の場合、ドテンできるように上限を引き上げる
                qty_limit = qty_limit + -self.position_size

        # 購入数をポジション最大サイズに抑える
        if qty_total > qty_limit:
            qty = qty - (qty_total - qty_limit)

        # 注文情報取得
        order = self.exchange.get_order(myid)

        # 前の注文が成り行き
        if order['type'] == 'market':
            # 約定するまで次の注文は受け付けない
            if order.status == 'open' or order.status == 'accepted':
                delta = datetime.utcnow() - order.accepted_at
                if delta < timedelta(seconds=60):
                    if not self.hft:
                        self.logger.info("REJECT: {0} order creating...".format(myid))
                    return
        else:
            if order.status == 'open' or order.status == 'accepted':
                # 前の注文と価格とサイズが同じなら何もしない
                if (abs(order.price - limit)<=limit_mask) and (order.amount == qty) and (order.side == side):
                    return
                # 新しい注文を制限する（指値を市場に出している最小時間を保証）
                if seconds_to_keep_order is not None:
                    past = datetime.utcnow() - order.accepted_at
                    if past < timedelta(seconds=seconds_to_keep_order):
                        return

            # 安全な空の旅
            if self.settings.safe_order:
                # 前の注文が注文一覧にのるまで次の注文は受け付けない
                if (order.status == 'accepted'):
                    delta = datetime.utcnow() - order.accepted_at
                    if delta < timedelta(seconds=60):
                        if not self.hft:
                            self.logger.info("REJECT: {0} order creating...".format(myid))
                        return
                # 同じIDのオープン状態の注文が2つ以上ある場合、注文は受け付けない（2つ前の注文がキャンセル中）
                orders = {k:v for k,v in self.exchange.get_open_orders().items() if v['myid']==myid}
                if len(orders) >= 2:
                    if not self.hft:
                        self.logger.info("REJECT: {0} too many orders...".format(myid))
                    return
            # 前の注文がオープンならキャンセル
            if (order.status == 'open') or (order.status == 'accepted'):
                self.exchange.cancel(myid)

        # 最小発注サイズ(FX 0.01/現物・先物は0.001)に切り上げる
        symbol = symbol or self.settings.symbol
        if symbol == 'FX_BTC_JPY':
            min_qty = 0.01
        else:
            min_qty = 0.001

        # 新規注文
        if qty > 0:
            qty = max(qty, min_qty)
            self.exchange.create_order(myid, side, qty, limit, stop, time_in_force, minute_to_expire, symbol)

    def get_order(self, myid):
        return self.exchange.get_order(myid)

    def get_open_orders(self):
        return self.exchange.get_open_orders()

    def entry(self, myid, side, qty, limit=None, stop=None, time_in_force = None, minute_to_expire = None, symbol = None, limit_mask = 0, seconds_to_keep_order = None):
        """注文"""

        # 買いポジションがある場合、清算する
        if side=='sell' and self.position_size > 0:
            qty = qty + self.position_size

        # 売りポジションがある場合、清算する
        if side=='buy' and self.position_size < 0:
            qty = qty - self.position_size

        # 注文
        self.order(myid, side, qty, limit, stop, time_in_force, minute_to_expire, symbol, limit_mask)

    def create_rich_ohlcv(self, ohlcv):
        if self.settings.disable_rich_ohlcv:
            rich_ohlcv = dotdict()
            for k in ohlcv[0].keys():
                rich_ohlcv[k] = [v[k] for v in ohlcv]
        else:
            rich_ohlcv = pd.DataFrame.from_records(ohlcv, index="created_at")
        return rich_ohlcv

    def calculate_sfd(self, ticker, executions, ticker_btcjpy, executions_btcjpy):
        if len(executions)>0:
            fx_ltp = executions[-1]['price']
        else:
            fx_ltp = ticker['ltp']
        if len(executions_btcjpy)>0:
            btc_jpy_ltp = executions_btcjpy[-1]['price']
        else:
            btc_jpy_ltp = ticker_btcjpy['ltp']
        sfd = dotdict()
        sfd.pct = ((fx_ltp / btc_jpy_ltp) * 100) - 100
        sfd.fee = 0
        SFD = [(5, 0.25), (10, 0.50), (15, 1.00), (20, 2.00)]
        for pct, fee in SFD:
            if sfd.pct >= pct:
                sfd.fee = fee
        return sfd

    def setup(self):
        # 実行中フラグセット
        self.running = True

        # 取引所セットアップ
        self.exchange = Exchange(apiKey=self.settings.apiKey, secret=self.settings.secret)
        if self.settings.use_lightning:
            self.exchange.enable_lightning_api(
                self.settings.lightning_userid,
                self.settings.lightning_password)
        self.exchange.start()

        # ストリーミング開始
        self.streaming = Streaming()
        self.streaming.start()
        self.ep = self.streaming.get_endpoint(self.settings.symbol, ['ticker', 'executions'],
            timeframe=self.settings.timeframe,
            max_ohlcv_size=self.settings.max_ohlcv_size)
        self.ep.wait_for(['ticker'])

        # 約定履歴・板差分から注文状態監視
        # ep = self.streaming.get_endpoint(self.settings.symbol, ['executions', 'board'])
        ep = self.streaming.get_endpoint(self.settings.symbol, ['executions'])
        self.exchange.start_monitoring(ep)
        self.monitoring_ep = ep

        # SFD計算用に現物価格も購読
        # self.fx_btc = (self.settings.symbol == 'FX_BTC_JPY')
        # if self.fx_btc:
        #     self.streaming.subscribe(product_id='BTC_JPY', topics=['ticker', 'executions'])
        #     self.streaming.wait_for(product_id='BTC_JPY', topics=['ticker', 'executions'])

        # 売買ロジックセットアップ
        if self.yoursetup:
            self.yoursetup(self)

    def start(self):
        self.logger.info("Start Trading")
        self.setup()

        def async_inverval(func, interval, parallels):
            next_exec_time = 0
            @wraps(func)
            def wrapper(*args, **kargs):
                nonlocal next_exec_time
                f_result = None
                t = time()
                if t > next_exec_time:
                    next_exec_time = ((t//interval)+1)*interval
                    f_result = func(*args,**kargs)
                    if parallels is not None:
                        parallels.append(f_result)
                return f_result
            return wrapper

        def async_result(f_result, last):
            if f_result is not None and f_result.done():
                try:
                    return None, f_result.result()
                except Exception as e:
                    self.logger.warning(e)
                    f_result = None
            return f_result, last

        self.hft = self.settings.interval < 3
        async_requests = []
        fetch_balance = async_inverval(self.exchange.fetch_balance, 15, async_requests)
        fetch_board_state = async_inverval(self.exchange.fetch_board_state, 3, async_requests)
        fetch_position = async_inverval(self.exchange.fetch_position, 3, async_requests)
        fetch_collateral = async_inverval(self.exchange.fetch_collateral, 15, async_requests)
        check_order_status = async_inverval(self.exchange.check_order_status, 3, async_requests)
        errorWait = 0
        f_collateral = f_balance = f_state = f_position = f_check = None
        collateral = balance = board_state = position = None
        once = True

        while True:
            self.interval = self.settings.interval

            try:
                # 注文処理の完了待ち
                self.exchange.wait_for_completion()

                # 待ち時間
                self.monitoring_ep.suspend(False)
                if self.interval:
                    if not self.hft:
                        self.logger.info("Waiting...")
                    wait_sec = (-time() % self.interval) or self.interval
                    sleep(wait_sec)
                self.monitoring_ep.suspend(True)

                # 例外発生時の待ち
                no_needs_err_wait = (errorWait == 0) or (errorWait < time())

                # ポジション等の情報取得
                if no_needs_err_wait:
                    # f_state = f_state or fetch_board_state(self.settings.symbol)
                    f_balance = f_balance or fetch_balance()
                    if self.settings.use_lightning:
                        f_position = f_position or fetch_position(self.settings.symbol)
                    else:
                        f_position = f_position or fetch_position(self.settings.symbol)
                        f_collateral = f_collateral or fetch_collateral()
                    f_check = f_check or check_order_status(show_last_n_orders=self.settings.show_last_n_orders)

                    # リクエスト完了を待つ
                    if not self.hft or once:
                        for f in concurrent.futures.as_completed(async_requests):
                            pass
                        once = False
                    async_requests.clear()

                    # 板状態取得
                    f_state, board_state = async_result(f_state, board_state)

                    # 資金情報取得
                    f_balance, balance = async_result(f_balance, balance)

                    # 建玉取得
                    if self.settings.use_lightning:
                        f_position, res = async_result(f_position, (position, collateral))
                        position, collateral = res
                    else:
                        f_position, position = async_result(f_position, position)

                    # 内部管理のポジション数をAPIで取得した値に更新
                    if 'checked' not in position:
                        self.exchange.restore_position(position.all)
                        position['checked'] = True

                    # 内部管理のポジション数取得
                    self.position_size, self.position_avg_price, _ = self.exchange.get_position()

                    # 証拠金取得
                    f_collateral, collateral = async_result(f_collateral, collateral)

                    # 注文情報取得
                    f_check, _ = async_result(f_check, None)

                    # REST API状態取得
                    self.api_state, self.api_avg_responce_time = self.exchange.api_state()
                    if self.api_state is not 'normal':
                        self.logger.info("REST API: {0} ({1:.1f}ms)".format(self.api_state, self.api_avg_responce_time*1000))

                # 価格データ取得
                ticker = dotdict(self.ep.get_ticker())
                ohlcv = executions = None
                # インターバルが0の場合、約定履歴の到着を待つ
                if self.settings.interval==0:
                    self.ep.wait_any(['executions'], timeout=0.5)
                # OHLCVを作成しない場合、約定履歴を渡す
                if self.settings.disable_create_ohlcv:
                    executions = self.ep.get_executions()
                else:
                    if self.settings.use_lazy_ohlcv:
                        ohlcv = self.create_rich_ohlcv(self.ep.get_lazy_ohlcv())
                    else:
                        ohlcv = self.create_rich_ohlcv(self.ep.get_boundary_ohlcv())

                # SFD価格剥離率
                # if self.fx_btc:
                #     ticker_btcjpy = dotdict(self.ep.get_ticker(product_id='BTC_JPY'))
                #     executions_btcjpy = self.ep.get_executions(product_id='BTC_JPY')
                #     sfd = self.calculate_sfd(ticker, executions, ticker_btcjpy, executions_btcjpy)
                # else:
                #     ticker_btcjpy = None
                #     executions_btcjpy = None
                #     sfd = dotdict({'pct':1, 'fee':0})

                # 売買ロジック呼び出し
                if no_needs_err_wait:
                    self.yourlogic(
                        ticker=ticker,
                        executions=executions,
                        ohlcv=ohlcv,
                        position=position,
                        collateral=collateral,
                        balance=balance,
                        board_state=board_state,
                        strategy=self)
                    errorWait = 0
                else:
                    self.logger.info("Waiting for Error...")

            except ccxt.DDoSProtection as e:
                self.logger.warning(type(e).__name__ + ": {0}".format(e))
                errorWait = time() + 60
            except ccxt.RequestTimeout as e:
                self.logger.warning(type(e).__name__ + ": {0}".format(e))
                errorWait = time() + 30
            except ccxt.ExchangeNotAvailable as e:
                self.logger.warning(type(e).__name__ + ": {0}".format(e))
                errorWait = time() + 5
            except ccxt.AuthenticationError as e:
                self.logger.warning(type(e).__name__ + ": {0}".format(e))
                self.private_api_enabled = False
                errorWait = time() + 5
            except ccxt.ExchangeError as e:
                self.logger.warning(type(e).__name__ + ": {0}".format(e))
                errorWait = time() + 5
            except (KeyboardInterrupt, SystemExit):
                self.logger.info('Shutdown!')
                break
            except Exception as e:
                self.logger.exception(e)
                errorWait = time() + 1

        self.logger.info("Stop Trading")
        # 停止
        self.running = False
        # ストリーミング停止
        self.streaming.stop()
        # 取引所停止
        self.exchange.stop()
