  
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

# time duration for trading
trading_start_time_hour= "06"
trading_end_time_hour = "22"


if not os.path.exists('results'):
        os.makedirs('results')

if not os.path.exists('datasets'):
        os.makedirs('datasets')
        
save_path = 'results/'
filename_results = datetime.datetime.now().strftime("%Y%m%d")
completeName = os.path.join(save_path, filename_results+".txt")

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
       start = datetime.datetime.today() - datetime.timedelta(7)
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
     dataframes = {}
     gain_day={}
     start = datetime.datetime.today() - datetime.timedelta(7)
     end = datetime.datetime.today()
     for filename in os.listdir('datasets'):
            symbol = filename.split(".")[0]
            
            df = pandas.read_csv('datasets/{}'.format(filename))
            if df.empty: 
                continue       
            df.dropna(axis = 0, inplace = True) # remove any null rows 
            df['hourly_pc'] = (df['Close'] /df['Close'].shift(1) -1)*100
            df['EMA_short'] = df['Close'].ewm(span = short_window, adjust = False).mean()
            df['EMA_long'] = df['Close'].ewm(span = long_window, adjust = False).mean()
            df['DMA_short'] = 2*df['EMA_short'] - (df['EMA_short'].ewm(span = short_window, adjust = False).mean())
            df['DMA_long'] = 2*df['EMA_long'] - (df['EMA_long'].ewm(span = long_window, adjust = False).mean())
            df['TR'] = abs(df['High'] - df['Low'])
            df['ATR'] = df['TR'].rolling(window=short_window).mean()
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
                    rows = t.find_all("span", {"class" : "Trsdu(0.3s) Fw(500) Pstart(10px) Fz(24px) C($positiveColor)"})
                    for row in rows:
                        temp_dir[row.get_text(separator=' ').split(" ")[1]]=row.get_text(separator=' ').split(" ")[1]
                
                #combining all extracted information with the corresponding ticker
             
                gain_day =temp_dir
                
            except Exception:
                    pass
 
            if gain_day == "":
                gain_day = " "
            
            
            try:
                    f = open(completeName, "a")
                    if df.iloc[-1]['Position'] == 1 or df.iloc[-1]['Position'] == -1 :
                         print("{0} is in crossover. Close = {1},Result = {2}, Volume = {3}, NATR = {4}, hourly_pc ={5},Daily Gain ={6} \n".format(symbol,df.iloc[-1]['Close'],df.iloc[-1]['Buy_Sell'],df.iloc[-1]['Volume'],df.iloc[-1]['Percent_ATR'],df.iloc[-1]['hourly_pc'], gain_day  ), file=f)
                         print("{0} is in crossover. Close = {1},Result = {2}, Volume = {3}, NATR = {4}, hourly_pc ={5},Daily Gain ={6} \n".format(symbol,df.iloc[-1]['Close'],df.iloc[-1]['Buy_Sell'],df.iloc[-1]['Volume'],df.iloc[-1]['Percent_ATR'],df.iloc[-1]['hourly_pc'],gain_day ))
                            
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
    message["To"] = receiver_email
    message["Subject"] = subject
    # message["Bcc"] = receiver_email  # Recommended for mass emails
    
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

def download_and_email():
    print ("RUNNING !!!" ) 
    if (datetime.datetime.today().weekday() <= 4) and ((datetime.datetime.now().hour >= int(trading_start_time_hour)) and (datetime.datetime.now().hour <= int(trading_end_time_hour)))== True:
        createdirectory()
        delete_results()
        f = open(completeName, "a")
        print ("Start SP500: %s\n" % time.ctime(), file=f) 
        print ("*******************************************************************" , file=f)
        f.close()
        yfinancedownload('input.csv','1h')
        dema_buy_sell_detect()
        email_export()
        

def main():
    download_and_email()
    schedule.every().hour.do(download_and_email)
    schedule.every().monday.at(trading_start_time_hour+":00").do(download_and_email)
    schedule.every().monday.at(trading_end_time_hour+":00").do(download_and_email)
    schedule.every().tuesday.at(trading_start_time_hour+":00").do(download_and_email)
    schedule.every().tuesday.at(trading_end_time_hour+":00").do(download_and_email)
    schedule.every().wednesday.at(trading_start_time_hour+":00").do(download_and_email)
    schedule.every().wednesday.at(trading_end_time_hour+":00").do(download_and_email)
    schedule.every().thursday.at(trading_start_time_hour+":00").do(download_and_email)
    schedule.every().thursday.at(trading_end_time_hour+":00").do(download_and_email)
    schedule.every().friday.at(trading_start_time_hour+":00").do(download_and_email)
    schedule.every().friday.at(trading_end_time_hour+":00").do(download_and_email)


    while True:
        schedule.run_pending()
        time.sleep(1)    
    
if __name__ == "__main__":
    main()