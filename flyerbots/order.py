# -*- coding: utf-8 -*-
import threading
from collections import OrderedDict, deque
from .utils import dotdict

class OrderManager:

    INVALID_ORDER = dotdict({
        'No':0,
        'myid':'__INVALID_ORDER__',
        'id':'__INVALID_ORDER__',
        'datetime':'2018-9-1T0:00:00.000',
        'status':'closed',
        'symbol':'FX_BTC_JPY',
        'type':'market',
        'side':'none',
        'price':0,
        'average_price':0,
        'amount':0,
        'filled':0,
        'remaining':0,
        'fee':0,
    })

    def __init__(self):
        self.orders = OrderedDict()
        self.lock = threading.Lock()
        self.number_of_orders = 0
        self.positions = deque()

    def add_order(self, new_order):
        with self.lock:
            self.number_of_orders += 1
            new_order['No'] = self.number_of_orders
            self.orders[new_order['id']] = new_order
        return new_order

    def add_position(self, p):
        # 建玉追加
        self.positions.append(p)
        while len(self.positions)>=2:
            r = self.positions.pop()
            l = self.positions.popleft()
            if r['side']==l['side']:
                # 売買方向が同じなら取り出したポジションを戻す
                self.positions.append(r)
                self.positions.appendleft(l)
                break
            else:
                if l['size'] >= r['size']:
                    # 決済
                    l['size'] = round(l['size'] - r['size'],8)
                    if l['size'] > 0:
                        # サイズが残っている場合、ポジションを戻す
                        self.positions.appendleft(l)
                else:
                    # 決済
                    r['size'] = round(r['size'] - l['size'],8)
                    if r['size'] > 0:
                        # サイズが残っている場合、ポジションを戻す
                        self.positions.append(r)

    def execute(self, o, e):
        updated = False
        with self.lock:
            if o['filled']<o['amount']:
                # ポジション追加
                self.add_position({'side':o['side'], 'size':e['size'], 'price':e['price']})
                # 注文情報更新
                last = o['filled'] * o['average_price']
                curr = e['size'] * e['price']
                filled =  round(o['filled'] + e['size'],8)
                average_price = (last + curr) / filled
                o['filled'] = filled
                o['average_price'] = average_price
                o['remaining'] = round(o['amount'] - filled,8)
                o['status'] = o['status']
                if o['remaining'] <= 0:
                    o['status'] = 'closed'
                else:
                    if o['status'] == 'accepted':
                        o['status'] = 'open'
                updated = True
        return updated

    def open_or_cancel(self, o, size):
        updated = False
        with self.lock:
            if o['status']=='cancel':
                # 板サイズが注文サイズ未満ならキャンセル完了
                if size<o['amount']:
                    o['status'] = 'canceled'
                    updated = True
            elif o['status']=='accepted':
                # 板サイズが注文サイズ以上ならオープン（他のユーザの注文と被る可能性があるが許容する）
                if size>=o['amount']:
                    o['status'] = 'open'
                    updated = True
        return updated

    def expire(self, o):
        with self.lock:
            if o['status'] in ['cancel', 'open']:
                o['status'] = 'canceled'

    def overwrite(self, o, latest):
        with self.lock:
            # ローカルで更新した状態は残しておく
            if latest['status'] == 'open':
                if o['status'] in ['cancel', 'canceled', 'closed']:
                    latest['status'] = o['status']
            if latest['filled'] < o['filled']:
                latest['filled'] = o['filled']
            for k in ['id', 'datetime', 'status', 'average_price', 'filled', 'remaining' ,'fee']:
                o[k] = latest[k]

    def cancel_order(self, myid):
        cancelable = ['open', 'accepted']
        with self.lock:
            my_orders = [v for v in self.orders.values() if (v['type'] != 'market') and (v['myid']==myid) and (v['status'] in cancelable)]
            for o in my_orders:
                o['status'] = 'cancel'
        return my_orders

    def cancel_order_all(self):
        cancelable = ['open', 'accepted']
        with self.lock:
            my_orders = [v for v in self.orders.values() if (v['type'] != 'market') and (v['status'] in cancelable)]
            for o in my_orders:
                o['status'] = 'cancel'
        return my_orders

    def get_order(self, myid):
        with self.lock:
            my_orders = [v for v in self.orders.values() if v['myid']==myid]
        if len(my_orders):
            return my_orders[-1]
        o = self.INVALID_ORDER.copy()
        o['myid'] = myid
        return dotdict(o)

    def get_open_orders(self):
        return self.get_orders(status_filter = ['open', 'accepted', 'cancel'])

    def get_orders(self, status_filter = None):
        with self.lock:
            if status_filter is None:
                my_orders = self.orders.copy()
            else:
                my_orders = {k:v for k,v in self.orders.items() if (v['status'] in status_filter)}
        return my_orders

    def cleaning_if_needed(self, limit_orders = 200, remaining_orders = 20):
        """注文情報整理"""
        open_status = ['open', 'accepted', 'cancel']
        with self.lock:
            if len(self.orders) > limit_orders:
                all_orders = list(self.orders.items())
                # open/accepted状態の注文は残す
                orders = [(k,v) for k,v in all_orders[:-remaining_orders] if v['status'] in open_status]
                # 最新n件の注文は残す
                orders.extend(all_orders[-remaining_orders:])
                self.orders = OrderedDict(orders)

    def printall(self):
        print('\n'+'\t'.join(self.INVALID_ORDER.keys()))
        for v in self.orders.values():
            print('\t'.join([str(v) for v in v.values()]))

if __name__ == "__main__":
    om = OrderManager()
    om.printall()

    o = OrderManager.INVALID_ORDER.copy()
    o['id'] = 'test1'
    o['status'] = 'open'
    om.add_order('L', o)
    print('add', om.get_order('L'))
    om.printall()

    o = OrderManager.INVALID_ORDER.copy()
    o['id'] = 'test2'
    o['status'] = 'open'
    om.add_order('S', o)
    print('add', om.get_order('S'))
    om.printall()

    o = om.get_order('L')
    o['filled'] = '1'
    o['remaining'] = '2'
    om.update_order(o)
    print('update', om.get_order('L'))
    om.printall()

    o = om.get_order('S')
    o['filled'] = '1'
    o['remaining'] = '2'
    om.update_order(o)
    print('update', om.get_order('S'))
    om.printall()

    print(om.cancel_order('L'))
    print('cancel', om.get_order('L'))
    om.printall()

    print(om.cancel_order('S'))
    print('cancel', om.get_order('L'))
    om.printall()

    print(om.get_orders())
