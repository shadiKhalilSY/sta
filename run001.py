import ujson as json
import websockets as ws
import asyncio
import nest_asyncio
from typing import Dict, Callable, Awaitable, Optional, List
import logging
import pandas as pd
from numerize import numerize
from datetime import  datetime, timezone, timedelta


nest_asyncio.apply()
logging.basicConfig(level=logging.INFO)



class ConnectWebsocket:
    MAX_RECONNECTS: int = 10
    def __init__(self,message_manger):
        self.log = logging.getLogger(__name__)
        self.conn = None
        self.ping_timeout = 60
        self.reconnect_attempts: int = 0
        self.socket: Optional[ws.client.WebSocketClientProtocol] = None
        self.message_manger = message_manger
            
            
        self.connect()
    
    def connect(self):
        self.conn = asyncio.ensure_future(self.run())
        #url = "wss://stream.binance.com:9443/ws"
        #async with websockets.connect(url) as ws:
            #await ws.send(json.dumps({"method": "SUBSCRIBE","params":["btcusdt@trade","ethusdt@trade"],"id": 1}))
            #print("connected")
            #self.wbs = ws
            #asyncio.create_task(self.listen())
            
    async def run(self):
        keep_waiting: bool = True

        logging.info("*** connecting *** to wss://stream.binance.com:9443/ws")
        try:
            async with ws.connect("wss://stream.binance.com:9443/ws") as socket:
                self.on_connect(socket)
                

                try:
                    while keep_waiting:
                        try:
                            evt = await asyncio.wait_for(self.socket.recv(), timeout=self.ping_timeout)
                        except asyncio.TimeoutError:
                            self.log.info("no message in {} seconds".format(self.ping_timeout))
                            print("need keep")
                            #await self.send_keepalive()
                        except asyncio.CancelledError:
                            self.log.error("cancelled error")
                            await self.ping()
                        else:
                            try:
                                evt_obj = json.loads(evt)
                            except ValueError:
                                pass
                            else:
                                await self.message_manger.msg_recv(evt_obj)
                except ws.ConnectionClosed as e:
                    self.log.info('conn closed:{}'.format(e))
                    keep_waiting = False
                    await self.reconnect()
                except Exception as e:
                    self.log.error(f'ws exception:{e}')
                    keep_waiting = False
                    await self.reconnect()
        except Exception as e:
            logging.info(f"websocket error: {e}")
                    
        
            
    def on_connect(self, socket):
        self.socket = socket
        self.reconnect_attempts = 0 
        
    async def reconnect(self):
        await self.cancel()
        self.reconnect_attempts += 1
        if self.reconnect_attempts < self.MAX_RECONNECTS:

            self.log.info(f"websocket reconnecting {self.MAX_RECONNECTS - self.reconnect_attempts} attempts left")
            reconnect_wait = 2
            self.log.info(f' waiting {reconnect_wait}s')
            await asyncio.sleep(reconnect_wait)
            self.connect()
        else:
            # maybe raise an exception
            self.log.error(f"websocket could not reconnect after {self.reconnect_attempts} attempts")
            pass
        
    async def cancel(self):
        try:
            self.conn.cancel()
        except asyncio.CancelledError:
            pass
        
    async def ping(self):
        await self.socket.ping()
    
    #async def send_keepalive(self):
        #msg = {"method": "keepAlive"}
        #await self.socket.send(json.dumps(msg, ensure_ascii=False))
    
    async def send_message(self, msg, retry_count=0):
        if not self.socket:
            if retry_count < 5:
                await asyncio.sleep(1)
                await self.send_message(msg, retry_count + 1)
            else:
                logging.info("Unable to send, not connected")
        else:
            await self.socket.send(json.dumps(msg, ensure_ascii=False)) 
        

            
        
        
class WebsocketManager:
    def __init__(self):
        self.conn = None
        self.loop = None
        self.log = logging.getLogger(__name__)
          
    async def create(self,message_manger):
        self.conn = ConnectWebsocket(message_manger)
    
    async def subscribe_trades(self, symbols: List[str]):
        req_msg = {"method": "SUBSCRIBE","params":symbols,"id": 1}
        await self.conn.send_message(req_msg)
        
    async def unsubscribe_trades(self, symbols: List[str]):
        req_msg = {"method": "UNSUBSCRIBE","params":symbols,"id": 1}
        await self.conn.send_message(req_msg)
        
    
            
    
class MessageManager:
    def __init__(self):
        self.trades = {}
        
    async def msg_recv(self,msg):
        if "e" in msg:
            if msg["e"] == "aggTrade":
                if msg["s"] not in self.trades:
                    self.trades[msg["s"]] = pd.DataFrame([],columns=["time","price","quantity","qusdt","bullish"])
                    
                self.trades[msg["s"]] = self.trades[msg["s"]].append(pd.Series([msg["E"],msg["p"],msg["q"],float(msg["p"])*float(msg["q"]),msg["m"]], index = self.trades[msg["s"]].columns ),ignore_index = True)
            else:
                print(msg)
        else:
            print(msg)
            
    async def buy_sell_pressure(self):
        while True:
            print(str(datetime.now()))
            for symbol, trades in self.trades.items():
                trades_file = f"data/{symbol}_trades.csv"
                if len(trades.index) > 0:
                    last_min_trades = trades[(trades['time'] < datetime.now(timezone.utc).replace(second=0, microsecond=0).timestamp()*1000) & (trades['time'] >= (datetime.now(timezone.utc).replace(second=0, microsecond=0)-timedelta(minutes=1)).timestamp()*1000)]
                    print(f'### {symbol} ### coins: {numerize.numerize(last_min_trades["quantity"].astype(float).sum())} value: ${numerize.numerize(last_min_trades["qusdt"].astype(float).sum())}')
                    trades = trades[trades.apply(lambda x: x.values.tolist() not in last_min_trades.values.tolist(), axis=1)]
                    last_min_trades.to_csv(trades_file,mode='a',header=False, index=False)
                    del last_min_trades
            print("================================")
            await asyncio.sleep(0.1 + float(str(datetime.now().replace(second=0, microsecond=0)+timedelta(minutes=1) - datetime.now()).split(":")[-1]))


async def main():
    mm = MessageManager()
    bws = WebsocketManager()
    await bws.create(mm)
    await bws.subscribe_trades(["btcusdt@aggTrade"])
    await bws.subscribe_trades(["ethusdt@aggTrade"])
    await bws.subscribe_trades(["bnbusdt@aggTrade"])
    task = asyncio.get_event_loop().create_task(mm.buy_sell_pressure())
    asyncio.get_event_loop().run_until_complete(task)
    #await asyncio.sleep(2)
    
    #await asyncio.sleep(20)
    #await bws.unsubscribe_trades(["scusdt@trade"])


if __name__ == "__main__":

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
