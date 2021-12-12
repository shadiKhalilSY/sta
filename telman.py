from telegram import *
from telegram.ext import *
from multiprocessing import Process
import time
import requests
import json 
import pandas as pd
from datetime import  datetime, timezone,timedelta
from numerize import numerize


users = 185714690
BASEURL="https://api.binance.com/api/v3/"
KLINEURL = BASEURL + "klines"
AGGTRADES = BASEURL + "aggTrades"
REFRESHTIME = 10000
symbol = "SHIBUSDT"
trades_file = f"data/{symbol}_trades.csv"

def dwonloadTrades(symbol,start):
    #print(f"start download {symbol} from {datetime.fromtimestamp(start/1000)}")
    #start_func_time = time.time()
    limit = '10000000'
    req_params = {"symbol" : symbol, 'startTime' : str(start),"endTime": str(start + 3600000), 'limit' : limit}
    req=requests.get(AGGTRADES, params = req_params)
    if req.status_code == 200:
        df= pd.DataFrame(json.loads(req.text))
        #df.to_csv(trades_file,mode='a',header=False, index=False)
        #trades= len(df.index)
        #downloaded_hours += 1
        #print(f"{(time.time() - start_func_time)}s for downloading {numerize.numerize(trades)}")
        df["q"] = df["q"].astype("float")
        df["p"] = df["p"].astype("float")
        df["m"] = df["m"].astype(int)
        df["usdt"] = round(df["q"] * df["p"],2)
        df.drop(['a', 'f', 'l', 'M'], axis=1, inplace=True)
        temp_cols=df.columns.tolist() # reposition "T" to the Begining
        index=df.columns.get_loc("T")
        new_cols=temp_cols[index:index+1] + temp_cols[0:index] + temp_cols[index+1:]
        df=df[new_cols]
        return df
    else:
        print(req.status_code)
        print(req.text)
        
class TeleManager:
    def __init__(self):
        
        self.updater = Updater('2101481233:AAFbA5DcL2OfbgWp2wxC9WUCmF2fHWvHdtE')
        self.updater.dispatcher.add_handler(CommandHandler('start', self.hello))
        self.updater.dispatcher.add_handler(CallbackQueryHandler(self.queryHandler))
        Process(target=self.getTrades).start()
        self.updater.start_polling()
        self.updater.idle()
        
        
    def hello(self, update: Update, context: CallbackContext) -> None:
        buttons = [[InlineKeyboardButton("🇬🇧 English",callback_data="lang_en")],[InlineKeyboardButton("🇸🇦 العربية",callback_data="lang_ar")]] 
        #update.message.reply_text(f'Hello {update.effective_user.first_name}')
        context.bot.send_message(chat_id=update.effective_chat.id,reply_markup=InlineKeyboardMarkup(buttons),text="Select your language")
     
    def queryHandler(self, update: Update, context:CallbackContext):
        query= update.callback_query.data
        update.callback_query.answer()
        update.callback_query.edit_message_text(text=f"Selected option:{query} \n user ID: {update.effective_chat.id}")
        print(query)
    
    
    def sendMsg(self):
        pass
    def getTrades(self):
        
        while True:
            recent_trades  = dwonloadTrades(symbol,int(datetime.now(timezone.utc).timestamp()*1000)-10000)
            #last_date = recent_trades.sort_values(by=['T']).iloc[-1,recent_trades.columns.get_loc('T')]
            recent_trades.to_csv(trades_file,mode='a',header=False, index=False)
            df_new = recent_trades.groupby(recent_trades['T'],as_index=False).aggregate({'p':'median','q':sum,'T':'first','m':'first','usdt':'sum'}).sort_values(by=['usdt'],ascending=False)
        
            for index, row in df_new[df_new["usdt"]>200000].iterrows():
                msg=f"{'🟢' if row['m'] == 0 else '🔴'} <b>#{symbol}</b> {'Buy Trade' if row['m'] == 0 else 'Sell Trade'}\n<b>Total:  ${numerize.numerize(row['usdt'])}💰</b>\nPrice:  ${row['p']}\nAmount:  {numerize.numerize(row['q'])} \n"
                self.updater.bot.send_message(chat_id=users,text=msg,parse_mode="HTML")
            time.sleep(10)


tm = TeleManager()
