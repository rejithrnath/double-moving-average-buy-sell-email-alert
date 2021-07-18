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

# time duration for trading
trading_start_time_hour= "09"
trading_end_time_hour = "22"


if not os.path.exists('results'):
        os.makedirs('results')

if not os.path.exists('datasets'):
        os.makedirs('datasets')
        
save_path = 'results/'
filename_results = datetime.datetime.now().strftime("%Y%m%d-%H")
completeName = os.path.join(save_path, filename_results+".txt")

def createdirectory():
    shutil.rmtree('datasets')
    os.makedirs('datasets')   
    

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
 
def dema_buy_sell_detect(symbol ="ETH-USD",short_window = 20, long_window = 50):
     dataframes = {}
     for filename in os.listdir('datasets'):
            symbol = filename.split(".")[0]
            
            df = pandas.read_csv('datasets/{}'.format(filename))
            if df.empty: 
                continue       
            df.dropna(axis = 0, inplace = True) # remove any null rows 

            df['SMA_short'] = df['Close'].rolling(window = short_window, min_periods = 1).mean()
            df['SMA_long'] = df['Close'].rolling(window = long_window, min_periods = 1).mean()
            df['DMA_short'] = 2*df['SMA_short'] - (df['SMA_short'].ewm(span = short_window, adjust = False).mean())
            df['DMA_long'] = 2*df['SMA_long'] - (df['SMA_long'].ewm(span = short_window, adjust = False).mean())
            df['Signal'] = 0.0
            df['Signal'] = np.where(df['DMA_short'] > df['DMA_long'], 1.0, 0.0) 
            df['Position'] = df['Signal'].diff()
            df['Buy_Sell'] = df['Position'].apply(lambda x: 'Buy' if x == 1 else 'Sell')
            
            try:
                    f = open(completeName, "a")
                    if df.iloc[-1]['Position'] == 1 or df.iloc[-1]['Position'] == -1 :
                         print("{0} is in crossover. Close = {1},Result = {2}, Volume = {3}\n".format(symbol,df.iloc[-1]['Close'],df.iloc[-1]['Buy_Sell'],df.iloc[-1]['Volume'] ), file=f)
                         print("{0} is in crossover. Close = {1},Result = {2}, Volume = {3}\n".format(symbol,df.iloc[-1]['Close'],df.iloc[-1]['Buy_Sell'],df.iloc[-1]['Volume'] ))
                            
                    f.close()
                            
            except Exception:
                    pass    




def email_export():
    from email import encoders
    from email.mime.base import MIMEBase
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    
    subject = "Results"+ str(datetime.datetime.now())
    body = "Email with attachment "
    
    sender_email = temp.config.sender_email
    receiver_email = temp.config.receiver_email
    password = temp.config.password

        
    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject
    message["Bcc"] = receiver_email  # Recommended for mass emails
    
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
    if (datetime.datetime.today().weekday() <= 4) and ((datetime.datetime.now().hour >= int(trading_start_time_hour)) and (datetime.datetime.now().hour <= int(trading_end_time_hour)))== True:
        createdirectory()
        f = open(completeName, "a")
        print ("Start SP500: %s" % time.ctime(), file=f) 
        f.close()
        yfinancedownload('input.csv','1h')
        dema_buy_sell_detect()
        email_export()
        
        createdirectory()
        f = open(completeName, "a")
        print ("Start OSL: %s" % time.ctime(), file=f) 
        f.close()
        yfinancedownload('inputOSL.csv','1h')
        dema_buy_sell_detect()
        email_export()


def main():
    # download_and_email()
    schedule.every().hour.do(download_and_email)
    # schedule.every().monday.at(trading_start_time_hour+":00").do(download_and_email)
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