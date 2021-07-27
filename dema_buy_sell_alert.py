  
# -*- coding: utf-8 -*-
"""
Created on Sun Jul 18 20:15:13 2021
@author: Rejith Reghunathan 
@email: rejithrnath@gmail.com
"""
import numpy as np 
import os, pandas
import datetime
import yfinance as yf
import shutil
import time
import email, smtplib, ssl
import schedule
import temp.config
import requests
from bs4 import BeautifulSoup



if not os.path.exists('results'):
        os.makedirs('results')

if not os.path.exists('datasets'):
        os.makedirs('datasets')
        
save_path = 'results/'
filename_results = datetime.datetime.now().strftime("%Y%m%d")
completeName = os.path.join(save_path, filename_results+".txt")

delta_time=7

def createdirectory():
    shutil.rmtree('datasets')
    os.makedirs('datasets') 
    
def delete_results():
    shutil.rmtree('results')
    os.makedirs('results')
    save_path = 'results/'
    filename_results = datetime.datetime.now().strftime("%Y%m%d")
    completeName = os.path.join(save_path, filename_results+".txt") 
      
    

def yfinancedownload(csv_file_name, interval_time):
       start = datetime.datetime.today() - datetime.timedelta(delta_time)
       end = datetime.datetime.today()
       with open(csv_file_name) as f:
            lines = f.read().splitlines()
            for symbol in lines:
                try:
                    data=yf.download(symbol,start,end, interval=interval_time, progress = True)
                    data.to_csv("datasets/{}.csv".format(symbol))
                except Exception:
                    pass
 
def dema_buy_sell_detect(symbol ="ETH-USD",short_window = 21, long_window = 55):
     gain_day = " "
     dataframes = {}
     start = datetime.datetime.today() - datetime.timedelta(delta_time)
     end = datetime.datetime.today()
     for filename in os.listdir('datasets'):
            symbol = filename.split(".")[0]
            
            df = pandas.read_csv('datasets/{}'.format(filename))
            if df.empty: 
                continue       
            df.dropna(axis = 0, inplace = True) # remove any null rows
             
            df['hourly_pc'] = (df['Close'] /df['Close'].shift(1) -1) *100
            df['EMA_short'] = df['Close'].ewm(span = short_window, adjust = False).mean()
            df['EMA_long'] = df['Close'].ewm(span = long_window, adjust = False).mean()
            df['DMA_short'] = 2*df['EMA_short'] - (df['EMA_short'].ewm(span = short_window, adjust = False).mean())
            df['DMA_long'] = 2*df['EMA_long'] - (df['EMA_long'].ewm(span = long_window, adjust = False).mean())
            df['H-L']=abs(df['High']-df['Low'])
            df['H-PC']=abs(df['High']-df['Adj Close'].shift(1))
            df['L-PC']=abs(df['Low']-df['Adj Close'].shift(1))
            df['TR']=df[['H-L','H-PC','L-PC']].max(axis=1,skipna=False)
            df['ATR'] = df['TR'].ewm(span=short_window,adjust=False,min_periods=short_window).mean()
            df['Percent_ATR'] = df['ATR']/df['Close']*100
            df['Signal'] = 0.0
            df['Signal'] = np.where(df['DMA_short'] > df['DMA_long'], 1.0, 0.0) 
            df['Position'] = df['Signal'].diff()
            df['Buy_Sell'] = df['Position'].apply(lambda x: 'Buy' if x == 1 else 'Sell')

            
             #Webscrapping
            try:
                temp_dir = {}
                url = 'https://finance.yahoo.com/quote/'+symbol+'/financials?p='+symbol
                headers={'User-Agent': "Mozilla/5.0"}
                page = requests.get(url, headers=headers)
                page_content = page.content
                soup = BeautifulSoup(page_content,'html.parser')
                tabl = soup.find_all("div", {"class" : "D(ib) Va(m) Maw(65%) Ov(h)"})
                for t in tabl:
                    rows = t.find_all("span", {"data-reactid" : "32"})
                    for row in rows:
                        temp_dir[row.get_text(separator=' ').split(" ")[1]]=row.get_text(separator=' ').split(" ")[1]
                
              
                gain_day =list(temp_dir.keys())[0]
                
                
            except Exception:
                    pass
 
           
           
            try:
                f = open(completeName, "a")
                if df.iloc[-1]['Position'] == 1 or df.iloc[-1]['Position'] == -1 :
                        print("{0} is in crossover. Close = {1:.2f}, Result = {2}, Volume = {3:.2f}, Percent ATR = {4:.2f} %, Hourly Gain ={5:.4f} %, Daily Gain ={6}  \n".format(symbol,df.iloc[-1]['Close'],df.iloc[-1]['Buy_Sell'],df.iloc[-1]['Volume'],df.iloc[-1]['Percent_ATR'],df.iloc[-1]['hourly_pc'] , gain_day ), file=f)
                        print("{0} is in crossover. Close = {1:.2f}, Result = {2}, Volume = {3:.2f}, Percent ATR = {4:.2f} %, Hourly Gain ={5:.4f} %, Daily Gain ={6}  \n".format(symbol,df.iloc[-1]['Close'],df.iloc[-1]['Buy_Sell'],df.iloc[-1]['Volume'],df.iloc[-1]['Percent_ATR'],df.iloc[-1]['hourly_pc'] , gain_day))
                        
                f.close()
                            
            except Exception:
                    pass
      


def email_export():
    from email import encoders
    from email.mime.base import MIMEBase
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    
    subject = "Results "+ str(datetime.datetime.now())
    body = "Email with attachment "
    
    sender_email = temp.config.sender_email
    receiver_email = temp.config.receiver_email
    password = temp.config.password

        
    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = sender_email
    message["Subject"] = subject
    message["Bcc"] = ", ".join(receiver_email)  # Recommended for mass emails
    
    # Add body to email
    message.attach(MIMEText(body, "plain"))
    
    
    # Open PDF file in binary mode
    with open(completeName, "rb") as attachment:
        # Add file as application/octet-stream
        # Email client can usually download this automatically as attachment
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())
    
    # Encode file in ASCII characters to send by email    
    encoders.encode_base64(part)
    
    # Add header as key/value pair to attachment part
    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {completeName}",
    )
    
    # Add attachment to message and convert message to string
    message.attach(part)
    text = message.as_string()
    
    # Log in to server using secure context and send email
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, text)
        print("Emailed!")
        delete_results()

def download_and_email():
    print ("RUNNING !!!" ) 
    createdirectory()
    f = open(completeName, "a")
    print ("Start SP500 -> %s \n" % time.ctime(), file=f) 
    print ("*******************************************************************" , file=f)
    f.close()
    yfinancedownload('SP500.csv','1h')
    dema_buy_sell_detect()
    f = open(completeName, "a")
    print ("*******************************************************************" , file=f)
    f.close()
    email_export()
    
    
    createdirectory()
    f = open(completeName, "a")
    print ("Start OL Stocks -> %s \n" % time.ctime(), file=f) 
    print ("*******************************************************************" , file=f)
    f.close()
    yfinancedownload('OSL.csv','1h')
    dema_buy_sell_detect()
    f = open(completeName, "a")
    print ("*******************************************************************" , file=f)
    f.close()
    email_export()
       

def main():
    
    print("RUNNING!!")
    download_and_email()
    trading_time = ["08","09","10","11","12","13","14","15","16","17","18","19","20","21"]
    for x in trading_time:
        schedule.every().monday.at(str(x)+":15").do(download_and_email)
        schedule.every().tuesday.at(str(x)+":15").do(download_and_email)
        schedule.every().wednesday.at(str(x)+":15").do(download_and_email)
        schedule.every().thursday.at(str(x)+":15").do(download_and_email)
        schedule.every().friday.at(str(x)+":15").do(download_and_email)
 
    while True:
        schedule.run_pending()
        time.sleep(1)    
    
if __name__ == "__main__":
    main()