
from binance import ThreadedWebsocketManager
from datetime import  datetime, timezone, timedelta
import pandas as pd


class BinanceWs:
    def __init__(self):
        self.twm = ThreadedWebsocketManager()
        self.twm.start()
        self.trades={}
        
        
    def startSocket(self,socket_type,pair):
        if(socket_type == "aggTrade"):
            self.twm.start_aggtrade_socket(callback=self.socketMessage, symbol=pair)
            
    def stopSocket(self,socket_type,pair):
        if(socket_type == "aggTrade"):
            self.twm.start_aggtrade_socket(callback=self.socketMessage, symbol=pair)
        
    def socketMessage(self,msg):
        if msg["e"] == "aggTrade":
            trades_file = f"data/{msg['s']}_trades.csv"
            if msg["s"] not in self.trades:
                self.trades[msg["s"]] = pd.DataFrame([[msg["E"],msg["p"],msg["q"],float(msg["p"])*float(msg["q"]),msg["m"]]],columns=["time","price","quantity","qusdt","bullish"])

            else:
                self.trades[msg["s"]] = self.trades[msg["s"]].append(pd.Series([msg["E"],msg["p"],msg["q"],float(msg["p"])*float(msg["q"]),msg["m"]], index = self.trades[msg["s"]].columns ),ignore_index = True)
            
            old_trades = self.trades[msg["s"]][(self.trades[msg["s"]]['time'] < (datetime.now(timezone.utc).timestamp()-60 )*1000)]
            
            if len(old_trades) > 0:
                old_trades.to_csv(trades_file,mode='a',header=False, index=False)
            self.trades[msg["s"]] = self.trades[msg["s"]][self.trades[msg["s"]].apply(lambda x: x.values.tolist() not in old_trades.values.tolist(), axis=1)]
            #print(self.trades[msg["s"]].shape[0])
        else:
            print(msg)
    def getBSP(self,symbol):
        buy_trades = self.trades[symbol][(self.trades[symbol]['time'] > 
                (datetime.now(timezone.utc).timestamp()-60*5 )*1000) & (self.trades[symbol]['bullish'] == False)]['qusdt'].sum()
        sell_trades = self.trades[symbol][(self.trades[symbol]['time'] > 
                (datetime.now(timezone.utc).timestamp()-60*5 )*1000) & (self.trades[symbol]['bullish'] == True)]['qusdt'].sum()
        return {"vol":buy_trades+sell_trades,"buy": buy_trades,"sell": sell_trades,"diff":(buy_trades-sell_trades)/(buy_trades+sell_trades)}
        
    def stopAllStreams(self):
        self.twm.stop()
        
        
if __name__ == "__main__":
    binws = BinanceWs()
    binws.startSocket("aggTrade","BTCUSDT")
    
