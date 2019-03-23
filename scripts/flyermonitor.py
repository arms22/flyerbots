# -*- coding: utf-8 -*-

from flyerbots.streaming import Streaming
from collections import deque
from time import sleep
from dateutil.parser import parse
from datetime import timedelta
from operator import itemgetter

attr = {
    'base':'\033[0m',
    'price':'\033[0m',
    'size':'\033[0m',
    'BUY':'\033[1;32m',
    'SELL':'\033[;31m',
    '':'\033[0m',
}

class flyerMonitor:

    def __init__(self):
        self.volume_imbalance = deque(maxlen=999)

    def start(self, procut_id, topics):
        streaming = Streaming()
        streaming.start()
        ep = streaming.get_endpoint(procut_id, topics)
        while True:
            try:
                ep.wait_any()
                self.show_tickers(ep.get_tickers())
                self.show_executions(ep.get_executions())
                self.show_boards(ep.get_boards())
            except (KeyboardInterrupt, SystemExit):
                break
        streaming.stop()

    def show_boards(self, boards):
        for b in boards:
            b['attr'] = attr['base']
            print('{attr}MID {mid_price}'.format(**b))
            asks = sorted(b['asks'], key=itemgetter('price'), reverse=True)
            for ask in asks:
                ask['attr'] = attr['SELL']
                print('{attr}ASK {price} {size:.2f}'.format(**ask))
            bids = sorted(b['bids'], key=itemgetter('price'), reverse=True)
            for bid in bids:
                bid['attr'] = attr['BUY']
                print('{attr}BID {price} {size:.2f}'.format(**bid))

    def show_tickers(self, tickers):
        for t in tickers:
            d = t.copy()
            d['spread'] = d['best_ask'] - d['best_bid']
            d['base_attr'] = attr['base']
            msg = "{base_attr}TICK {best_bid:7.0f}({spread:5.0f}){best_ask:7.0f} {best_bid_size:6.2f}|{best_ask_size:<6.2f}"
            print(msg.format(**d))

    def show_executions(self, executions):
        for e in executions:
            if e['side'] == 'BUY':
                self.volume_imbalance.append(e['size'])
            else:
                self.volume_imbalance.append(-e['size'])

            volume = sum(self.volume_imbalance)
            volume_bar = '#' * min(int((abs(volume) + 0.3) * 0.5), 14)
            volume_bar_attr = 'BUY' if volume > 0 else 'SELL'
            imbalance = sum(((v>0)*1)+((v<0)*-1) for v in self.volume_imbalance)

            d = e.copy()
            d['volume']=abs(volume)
            d['imbalance']=imbalance
            d['volume_bar']=volume_bar
            d['volume_bar_attr']=attr[volume_bar_attr]
            d['side_attr']=attr[e['side']]
            d['buy_attr']=attr['BUY']
            d['sell_attr']=attr['SELL']
            d['base_attr']=attr['base']

            msg = "{base_attr}EXEC {price:7.0f} {side_attr}{side:<4}({imbalance:<+4}) {base_attr}{size:5.2f}"
            msg+= "{volume_bar_attr} {volume:5.1f}|{volume_bar:<14}{base_attr}"
            print(msg.format(**d))

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--product_id", dest='product_id', type=str, default='FX_BTC_JPY')
    parser.add_argument("--topics", dest='topics', type=str, nargs='*', default=['executions', 'ticker'])
    args = parser.parse_args()

    flyerMonitor().start(args.product_id, args.topics)
