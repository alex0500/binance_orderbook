import websocket
import json
import logging
import requests
import multiprocessing
import platform
import sqlite3
import pandas

from concurrent.futures import ProcessPoolExecutor
from time import time
from flask import Flask, render_template
from livereload import Server  # tornado version <6.3

app= Flask(__name__)



class Constant:
    DB_FILE = 'binance.db'
    PAIR = 'BTCUSDT'


class Exchange_socket:
    ENDPOINT = 'wss://stream.binance.com/stream?streams=depth'
    SNAPSHOT = 'https://www.binance.com/api/v1/depth?symbol='
    DB_FILE = 'binance.db'


class Orderbook:
    def __init__(self):
        #self.xBook = {'bid':{}, 'ask':{}}  # State of orderbook
        self.xBids = {}
        self.xAsks = {}
        self.lastID = 0

    def __process(self):
        self.xAsks = dict(sorted(self.xAsks.items(), key=self.__tofloat))
        self.xBids = dict(sorted(self.xBids.items(), key=self.__tofloat, reverse=True))

    def __tofloat(self, oValue):
        return float(oValue[0])

    def reset(self, oSnapshot):
        self.lastID = oSnapshot['lastUpdateId']
        self.xBids = {}
        self.xAsks = {}
        for oBid in oSnapshot['bids']:
            self.xBids[str(oBid[0])] = str(oBid[1])
        for oAsk in oSnapshot['asks']:
            self.xAsks[str(oAsk[0])] = str(oAsk[1])
        #print(self.xAsks)
        self.__process()
        #print(self.xAsks)

    def update(self, oUpdate):
        try:
            for oBid in oUpdate['data']['b']:
                if str(oBid[1]) == '0.00000000':
                    #self.xBids.pop(str([oBid[0]]))
                    try:
                        del self.xBids[str([oBid[0]])]
                    except KeyError:
                        pass
                    continue
                self.xBids[str(oBid[0])] = str(oBid[1])
            for oAsk in oUpdate['data']['a']:
                if str(oAsk[1]) == '0.00000000':
                    #self.xAsks.pop([str(oAsk[0])])
                    try:
                        del self.xAsks[str(oAsk[0])]
                    except KeyError:
                        pass
                    continue
                self.xAsks[str(oAsk[0])] = str(oAsk[1])
            self.__process()
        except KeyError:
            print('error ')




# Creating payload for subscription with list of pairs
def create_payload(aPair):
    xParams = []
    aPair = str(aPair).lower()
    xParams.append(f'{aPair}@depth')

    return {
        "method": "SUBSCRIBE",
        "params": xParams,
        "id"    : 1
    }

def ws_connect(aPair):
    while True:
        oWs = websocket.create_connection(
            Exchange_socket.ENDPOINT,
            enable_multithread=False,
            skip_utf8_validation=True
        )
        oPayload_subcription = create_payload(aPair)
        oWs.send(json.dumps(oPayload_subcription))
        #oQueue.put((aType, oWs.recv()))
        #logging.info('Pairs: [%s][%s]', aType, xPairs)
        break
    return oWs



def process_data():
    oDB, oCursor = init_database(Constant.DB_FILE)
    create_table(oDB, Constant.PAIR)
    oOrderbook = Orderbook()
    while 1:
        oData = oQueue.get()
        #print(oData)
        if len(oData) == 3:
            oOrderbook.reset(oData[1])
            #print(oData[1])
            continue
        if oOrderbook.lastID == 0: continue

        oBook_update = json.loads(oData[1])
        if oBook_update.get('data') is None: continue
        if oBook_update['data']['u'] <= oOrderbook.lastID: continue
        oOrderbook.update(oBook_update)

        oUpdate_timestamp = {
        'local_timestamp': int(time() * 1000),  # timestamp in milisec resolution
        'raw_data'       : oBook_update
        }

        aSQL = f"""
            INSERT INTO {Constant.PAIR} (raws) VALUES (?)
                        """
        oDB.execute(aSQL, (json.dumps(oUpdate_timestamp),))
        oDB.commit()

        #pd = pandas.DataFrame(list(oOrderbook.xBids.items()), columns=['bid', 'quantity'])
        oBook.put((oOrderbook.xAsks, oOrderbook.xBids))
        #print(oBook_update)
        #print('ask:', oOrderbook.xAsks)
        #print('bid:', oOrderbook.xBids)
    return

def create_table(oDB, aTablename):
    while True:
        try:
            aSQL = f"""
            CREATE TABLE IF NOT EXISTS `{aTablename}` (raws TEXT)
            """
            oDB.execute(aSQL)
            oDB.commit()
            break
        except Exception as oErr:
            logging.error('Error 0339 [%s]', oErr)
    return


def connect_subscribe(aPair):
    oWs = ws_connect(aPair)
    while 1:
        oQueue.put((aPair, oWs.recv()))
    return


# Requesting snapshot of book
# aType is pair or future
def request_snapshot(aPair):
    aUrl_snapshot = f'{Exchange_socket.SNAPSHOT}{aPair}'
    while True:
        oRequest = requests.get(aUrl_snapshot)
        oRequest_json = json.loads(oRequest.content)
        oRequest.close()
        del oRequest
        if oRequest_json.get('bids') is None:
            continue
        oQueue.put((aPair, oRequest_json, 'snapshot'))
        break
    return oRequest_json

# initialization DB
def init_database(aFilename):
    oDB = sqlite3.connect(aFilename)
    oDB.row_factory = lambda cursor, row: json.loads(row[0])
    oCursor = oDB.cursor()
    oCursor.execute('''PRAGMA journal_mode = WAL''')
    oCursor.execute('''PRAGMA synchronous = NORMAL''')
    oCursor.execute('''PRAGMA locking_mode = EXCLUSIVE''')
    return oDB, oCursor

@app.route("/")
def hello_world():
    xAsk_Bid = oBook.get()

    oPD_ask = pandas.DataFrame(list(xAsk_Bid[0].items()), columns=['ask', 'quantity'])
    oPD_ask_reverse = oPD_ask.iloc[:, ::-1]

    oAsk_html = oPD_ask_reverse.to_html(index=False, justify='center')

    oPD_bid = pandas.DataFrame(list(xAsk_Bid[1].items()), columns=['bid', 'quantity'])
    oBid_html = oPD_bid.to_html(index=False, justify='center')
    return render_template('binance_orderbook.html', a=oAsk_html, b=oBid_html, pair=Constant.PAIR)


if __name__ == '__main__':
    if platform.system() == 'Darwin':  # Use for OSX
        multiprocessing.set_start_method('fork')

    logging.basicConfig(level=logging.INFO)

    oQueue = multiprocessing.Queue()
    oBook = multiprocessing.Queue()

    with ProcessPoolExecutor(max_workers=10) as oProcess:
        oProcess.submit(process_data)
        oProcess.submit(connect_subscribe, Constant.PAIR)
        oProcess.submit(request_snapshot, Constant.PAIR)
        server = Server(app.wsgi_app)
        server.serve()
        #app.run()
