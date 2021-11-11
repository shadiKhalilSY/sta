from binance import ThreadedWebsocketManager
from numerize import numerize
from datetime import  datetime, timezone, timedelta
import pandas as pd
import asyncio


class BinanceWs:
    def __init__(self):
        self.twm = ThreadedWebsocketManager()
        self.twm.start()
        self.trades={}
        self.bsp_on= False
        
        
    def startSocket(self,socket_type,pair):
        if(socket_type == "aggTrade"):
            self.twm.start_aggtrade_socket(callback=self.socketMessage, symbol=pair)
            if pair not in self.trades:
                self.trades[pair] = pd.DataFrame([],columns=["time","price","quantity","qusdt","bullish"])
            if not self.bsp_on:
                asyncio.create_task(self.buySellPressure())
                self.bsp_on = True
                print("BuySellPressure Turned On")
            
    def stopSocket(self,socket_type,pair):
        if(socket_type == "aggTrade"):
            self.twm.start_aggtrade_socket(callback=self.socketMessage, symbol=pair)
        
    def socketMessage(self,msg):
        #print(msg)
        if msg["e"] == "aggTrade":
            #trades_file = f"data/{msg['s']}_trades.csv"
            self.trades[msg["s"]] = self.trades[msg["s"]].append(pd.Series([msg["E"],msg["p"],msg["q"],float(msg["p"])*float(msg["q"]),msg["m"]], index = self.trades[msg["s"]].columns ),ignore_index = True)
            
            #old_trades = self.trades[msg["s"]][(self.trades[msg["s"]]['time'] < (datetime.now(timezone.utc).timestamp()-60 )*1000)]
            
            #if len(old_trades) > 0:
                #old_trades.to_csv(trades_file,mode='a',header=False, index=False)
            #self.trades[msg["s"]] = self.trades[msg["s"]][self.trades[msg["s"]].apply(lambda x: x.values.tolist() not in old_trades.values.tolist(), axis=1)]
            #print(self.trades[msg["s"]].shape[0])
        else:
            print(msg)
    def getBSP(self,symbol):
        buy_trades = self.trades[symbol][(self.trades[symbol]['time'] > 
                (datetime.now(timezone.utc).timestamp()-60*5 )*1000) & (self.trades[symbol]['bullish'] == False)]['qusdt'].sum()
        sell_trades = self.trades[symbol][(self.trades[symbol]['time'] > 
                (datetime.now(timezone.utc).timestamp()-60*5 )*1000) & (self.trades[symbol]['bullish'] == True)]['qusdt'].sum()
        return {"vol":buy_trades+sell_trades,"buy": buy_trades,"sell": sell_trades,"diff":(buy_trades-sell_trades)/(buy_trades+sell_trades)}
        #print(f"""VOL: {numerize.numerize(buy_trades+sell_trades)},
#BUY:{numerize.numerize(buy_trades)} / SELL {numerize.numerize(sell_trades)} / D: {numerize.numerize((buy_trades-sell_trades)/(buy_trades+sell_trades))}""")
    def stopAllStreams(self):
        self.twm.stop()
        
    async def buySellPressure(self):
        while True:
            print(str(datetime.now()))
            if len(self.trades["SCUSDT"].index) > 0:
                last_min_trades = self.trades["SCUSDT"][(self.trades["SCUSDT"]['time'] < datetime.now(timezone.utc).replace(second=0, microsecond=0).timestamp()*1000) & (self.trades["SCUSDT"]['time'] >= (datetime.now(timezone.utc).replace(second=0, microsecond=0)-timedelta(minutes=1)).timestamp()*1000)]
                print(f'coins: {last_min_trades["quantity"].astype(float).sum()} value: ${last_min_trades["qusdt"].astype(float).sum()}')
                print("==========================")
                self.trades["SCUSDT"] = self.trades["SCUSDT"][self.trades["SCUSDT"].apply(lambda x: x.values.tolist() not in last_min_trades.values.tolist(), axis=1)]
            await asyncio.sleep(0.1 + float(str(datetime.now().replace(second=0, microsecond=0)+timedelta(minutes=1) - datetime.now()).split(":")[-1]))
        
if __name__ == "__main__":
    binws = BinanceWs()
    binws.startSocket("aggTrade","SCUSDT")
    
