import websocket
import threading
import json
import logging
import asyncio
import time
from multiprocessing import Process
from datetime import  datetime, timezone, timedelta
from typing import Dict, Callable, Awaitable, Optional, List
logging.basicConfig(level=logging.INFO,format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='test.log',
                    filemode='w')

BASE_URL = "wss://stream.binance.com:9443/ws"
MAX_RECONNECTS: int = 10
recv_msg = 0
class WebsocketManger:
    def __init__(self):
        self.log = logging.getLogger("webs")
        self.ws = None
        self.ping_timeout = 60
        self.reconnect_attempts: int = 0
        print("start print_recv_msg")
        
        print("start connect")
        self.connect()
        Process(self.print_recv_msg()).start()
        
    def connect(self):
        self.log.info("trying to connect")
        try:
            self.ws = websocket.WebSocketApp(BASE_URL, on_open=self.on_open, on_message=self.on_message, on_error=self.on_error, on_close=self.on_close) 

            wst = threading.Thread(target=lambda: self.ws.run_forever())
            wst.daemon = False
            wst.start()
        except Exception as e:
            self.log.error(f"while connecting: {e}")
    
    def on_open(self,ws):
        print("open")
        print(self.ws)
        ws.send(json.dumps({"method": "SUBSCRIBE","params":["btcusdt@aggTrade","ethusdt@aggTrade","bnbusdt@aggTrade","solusdt@aggTrade"],"id": 1}))
    
    def on_message(self,ws,m):
        global recv_msg
        recv_msg += 1
    def on_close(self,ws, close_status_code, close_msg):
        self.log.info("connect closed")

    def on_error(self,ws, error): 
        self.log.error(f"Protocol problem: {error}") 
        
    def disconnect(self):
        self.ws.close()
        
    def print_recv_msg(self):
        global recv_msg
        while True:
            print(f"## {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ## {recv_msg} trades")
            self.log.info(f"{recv_msg} trades")
            recv_msg = 0
            time.sleep(float(str(datetime.now().replace(second=0, microsecond=0)+timedelta(minutes=1) - datetime.now()).split(":")[-1]))
            
            
        
        
        
wsm = WebsocketManger()
