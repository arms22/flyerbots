# coding: UTF-8
import requests
import logging
import time
import json
import threading
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import signal
import sys

class LightningError(Exception):
    pass

class LightningAPI:

    def __init__(self, id, password, timeout = 60):
        self.id = id
        self.password = password
        self.account_id = ''
        self.timeout = timeout
        self.api_url = 'https://lightning.bitflyer.com/api/trade'
        self.logger = logging.getLogger(__name__)
        self.session = requests.session()
        self.logon = False
        self.driver = None

        # #ブラウザを起ち上げっぱなしにしたいのでthreading
        # self.thread = threading.Thread(target=lambda: self.login())
        # self.thread.daemon = True
        # self.thread.start()

    def login(self):
        """ログイン処理"""
        try:
            #ヘッドレスブラウザがらみの設定など
            # WEB_DRIVER_PATH = './chromedriver.exe' #windows
            # WEB_DRIVER_PATH = './chromedriver' #mac linux
            #ヘッドレスブラウザのオプションを設定
            options = Options()
            # options.binary_location = 'C:/*********/chrome.exe' #windowsのみPATH指定
            options.add_argument('--headless') #ヘッドレスモードを有効、指定しなければ通常通りブラウザが立ち上がる
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-gpu')
            options.add_argument('--user-agent=Mozilla/5.0 (iPhone; U; CPU iPhone OS 5_1_1 like Mac OS X; en) AppleWebKit/534.46.0 (KHTML, like Gecko) CriOS/19.0.1084.60 Mobile/9B206 Safari/7534.48.3')

            # ヘッドレスブラウザ(webdriver)インスタンスを作成
            # driver = webdriver.Chrome(WEB_DRIVER_PATH, chrome_options=options)
            self.logger.info('Start WebDriver...')
            driver = webdriver.Chrome(chrome_options=options)

            # bitFlyerへアクセス
            self.logger.info('Access lightning...')
            driver.get('https://lightning.bitflyer.jp/')
            # driver.save_screenshot("login.png")

            # ログインフォームへID、PASSを入力
            login_id = driver.find_element_by_id('LoginId')
            login_id.send_keys(self.id)
            login_password = driver.find_element_by_id('Password')
            login_password.send_keys(self.password)

            # ログインボタンをクリック(2段階認証が無い場合はログイン完了)
            self.logger.info('Login lightning...')
            driver.find_element_by_id('login_btn').click()
            # driver.save_screenshot("2factor.png")

            # 通常2段階認証の処理が入るが種類が多いので割愛
            print("Input 2 Factor Code >>")
            driver.find_element_by_name("ConfirmationCode").send_keys(input())

            # 確認ボタンを押す
            driver.find_element_by_xpath("/html/body/main/div/section/form/button").click()
            # driver.save_screenshot("trade.png")

            # account_idを取得(実は必要ない、たぶん)
            self.account_id = driver.find_element_by_tag_name('body').get_attribute('data-account')

            # ヘッドレスブラウザで取得したcookieをrequestsにセット
            for cookie in driver.get_cookies():
                self.session.cookies.set(cookie['name'], cookie['value'])

            self.logon = True

            driver.get('https://lightning.bitflyer.jp/performance')
            # driver.save_screenshot("performance.png")

            self.logger.info('Lightning API Ready')
            self.driver = driver

            # ヘッドレスブラウザを起ち上げっぱなしにしたいので、とりあえずループさせる？
            # cookieを定期的に更新させてもよいのかもしれない
            # while True:
            #     pass

        except Exception as e:
            self.logger.warning(type(e).__name__ + ": {0}".format(e))

    def logoff(self):
        self.logger.info('Lightning Logoff')
        self.driver.quit()

    def sendorder(self, product_code, ord_type, side, price, size, minuteToExpire = 43200, time_in_force = 'GTC'):
        """注文送信"""
        params = {
            'account_id': self.account_id,
            'is_check': 'false',
            'lang': 'ja',
            'minuteToExpire': minuteToExpire,
            'ord_type': ord_type,
            'price': price,
            'product_code': product_code,
            'side': side,
            'size': size,
            'time_in_force': time_in_force,
        }
        return self.do_request('/sendorder', params)

    def getMyActiveParentOrders(self, product_code):
        """注文取得(アクティブ)"""
        params = {
            'account_id': self.account_id,
            'lang': 'ja',
            'product_code': product_code
        }
        return self.do_request('/getMyActiveParentOrders', params)

    def getMyBoardOrders(self, product_code):
        """注文取得(全て/キャンセルが含まれるかも)"""
        params = {
            'account_id': self.account_id,
            'lang': 'ja',
            'product_code': product_code
        }
        return self.do_request('/getMyBoardOrders', params)

    def cancelorder(self, product_code, order_id):
        """注文キャンセル"""
        params = {
            'account_id': self.account_id,
            'lang': 'ja',
            'order_id': order_id,
            'parent_order_id': '',
            'product_code': product_code
        }
        return self.do_request('/cancelorder', params)

    def cancelallorder(self, product_code):
        """注文全キャンセル"""
        params = {
            'account_id': self.account_id,
            'lang': 'ja',
            'product_code': product_code
        }
        return self.do_request('/cancelallorder', params)

    def getmyCollateral(self, product_code):
        """証拠金の状態やポジションを取得"""
        params = {
            'account_id': self.account_id,
            'lang': 'ja',
            'product_code': product_code
        }
        return self.do_request('/getmyCollateral', params)

    def inventories(self):
        """資産情報取得"""
        params = {
            'account_id': self.account_id,
            'lang': 'ja',
        }
        return self.do_request('/inventories', params)

    def do_request(self, endpoint, params):
        """リクエスト送信"""
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'X-Requested-With':'XMLHttpRequest'
        }

        response = self.session.post(self.api_url + endpoint,
            data=json.dumps(params), headers=headers, timeout=self.timeout)

        content = ''
        if len(response.content) > 0:
            content = json.loads(response.content.decode("utf-8"))
            if isinstance(content, dict):
                if 'status' in content:
                    if content['status'] < 0:
                        raise LightningError(content)
                return content['data']

        return content


if __name__ == '__main__':

    ID = ''
    PASS = ''
    PRODUCT_CODE = 'FX_BTC_JPY'
    BUY = 'BUY'
    SELL = 'SELL'
    LIMIT = 'LIMIT'
    MARKET = 'MARKET'

    #取引所インスタンスを作成
    bitflyer = LightningAPI(ID, PASS)
    bitflyer.login()

    #ログイン完了待ち
    while True:
        if bitflyer.logon:
            break

    #↓以下はサンプルコード↓

    #注文送信
    order = bitflyer.sendorder(PRODUCT_CODE, LIMIT, BUY, 600000, 0.01)
    order_ref_id = order['order_ref_id']

    #注文結果を表示(order_ref_id)
    print('send order: ' + order_ref_id)

    #注文取得可能になるまで待機
    time.sleep(2)

    #注文取得
    orders = bitflyer.getMyActiveParentOrders(PRODUCT_CODE)

    #注文結果を表示(order_ref_id)
    for o in orders:
       print(o['order_ref_id'], o['order_id'])
       #さっき注文したオーダーIDを取得
       if o['order_ref_id'] == order_ref_id:
           order_id = o['order_id']

    #注文キャンセル
    cancel = bitflyer.cancelorder(PRODUCT_CODE, order_id)
    print('cancel order: ' + order_id)

    # #注文10回送信
    # orders_id = []
    # for i in range(0, 10):
    #    order = bitflyer.sendorder(PRODUCT_CODE, LIMIT, BUY, 600000 - (i * 1000), 0.01)
    #    print('send order: ' + order['order_ref_id'])

    # #遅延するかもしれないので注文反映されるまで適当に待機
    # time.sleep(2)

    # #注文全キャンセル
    # bitflyer.cancelallorder(PRODUCT_CODE)

    # #建玉の取得
    # positions = bitflyer.getmyCollateral(PRODUCT_CODE)
    # for p in positions['positions']:
    #    print('position: ', p['product_code'], p['side'], p['price'], p['size'])
