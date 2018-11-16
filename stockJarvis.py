# -*- coding: utf-8 -*-
"""
Created on Fri Sep 21 23:35:26 2018

@author: clim

Learn and built on 
https://github.com/JECSand/yahoofinancials
https://stackoverflow.com/questions/49705047/downloading-mutliple-stocks-at-once-from-yahoo-finance-python

"""
#explore multithread to fasten long process of getting today's data (roughly 10s per stock)
#unique date&name (both texts) for SQL not working -> mitigated by dataframe drop duplicates
#

from yahoofinancials import YahooFinancials
import pandas as pd
import json
import sqlite3
import datetime
#import matplotlib.pyplot as plt
import numpy as np

import stocklistfile    # import stock list from stocklist.py

# Stock lists

# columns to get for daily data
dailycol = {'regularMarketOpen':'open',
            'regularMarketPrice':'close',
            'regularMarketDayLow':'low',
            'regularMarketDayHigh':'high',
            'regularMarketVolume':'volume',
            }

# Function to clean data extracts
def clean_stock_data(stock_data_list):
    new_list = []
    for rec in stock_data_list:
        if 'type' not in rec.keys():
            new_list.append(rec)
    return new_list

# Function to construct data frame based on a stock and it's market index
def build_data_frame(data_json_list):
    data_text = json.dumps(data_json_list)
    df = pd.read_json(data_text)
    return df

# Function to select stock list and database based on selected region
def selectmarket(exchange):
    stockdb = stocklistfile.stockdb[exchange]
    stocklist = stocklistfile.stocklist[exchange]
    return stocklist, stockdb
            
# Select exchange
exchange = 'SG'    
    
# Select mode
first_time = False   #Set to true only for first time to create the database
read_data = True    #If read_data is true, will not get any data from yahoofinancials
get_archive = True   #If true then get archive, else get today's data (after market close)

# Set date range for archive data
freq = 'daily'
start_date = '2018-11-14'
end_date = '2018-12-31'

# Set Parameters
value_fil = 100000   # value filter (minimum transacted value, for daily rocket analysis and MA analysis)
ma = [5,10,20,50,90] # days of MA to compute

################################### CODES #####################################################

stocklist, database = selectmarket(exchange)
    
#create database table if first_time
if first_time:
    conn = sqlite3.connect(database)
    cur = conn.cursor()
    #create a table with unique constraints combination of date and name, i.e each stock only have one record for each day
    cur.execute('CREATE TABLE table1 (name TEXT, date TEXT, open REAL, close REAL, low REAL, high REAL, volume REAL, CONSTRAINT unq UNIQUE (date,name))')  
    conn.close()
elif not read_data:    
    # Get stock data
    df_stock = pd.DataFrame()
    today = datetime.datetime.now().strftime("%Y-%m-%d")  #get today's date in str
    
    print("Retrieving data of "+str(len(stocklist))+" stocks...")
    timea = datetime.datetime.now()
    for keys in stocklist:
        if get_archive:
        #get archive data
            print(keys)
            datarec = clean_stock_data(YahooFinancials(stocklist[keys]).get_historical_price_data(start_date, end_date, freq)[stocklist[keys]]['prices'])
            if not datarec:
                print("No data available for selected period")
                break
            df = build_data_frame(datarec)
            df = df.drop(columns=['adjclose','date']) #drop adjusted close price and date
            df.rename(columns={'formatted_date':'date'},inplace=True) #use formatted_date as the only date info
            df['name'] = keys #add column stockname        
            df_stock = df_stock.append(df)
        else:
        #get today's data
            print(keys)            
            datarec = YahooFinancials(stocklist[keys]).get_stock_price_data()
            if not datarec:
                print("No data available for today")
                break
            df = build_data_frame(datarec)
            df = df.T[list(dailycol.keys())] #get relevant columns
            df.rename(columns=dailycol,inplace=True) #rename them according to database column names
            df['name'] = keys  #add column stockname
            df['date'] = today #add column today's date
            df_stock = df_stock.append(df)
    
    timeb = datetime.datetime.now()
    if not df_stock.empty:
        print("Retrieved "+ str(len(df_stock.date.unique()))+ " days records of " + str(len(df_stock.name.unique())) +" stocks in " + str(timeb-timea))
        # Write stock data to database
        conn = sqlite3.connect(database)
        cur = conn.cursor()
        df_stock.to_sql("table1",conn,if_exists='append',index=False)
        
else:
         
#   # Read stock data from database
    conn = sqlite3.connect(database)
    cur = conn.cursor()
    print("Accessing Stark Industries' central database...")
    data = pd.read_sql_query("SELECT * FROM table1",conn)
    data = data.drop_duplicates()
    print("Latest record date is "+np.sort(data.date.unique())[-1])
    firstrecord = np.sort(data.date.unique())[0]
    print("Oldest record date is "+firstrecord)
    
    if len(data.date.unique()) < np.max(ma):
        print("Jarvis: Sir, no enough days to compute your largest MA. Please run get_archive mode")
        print("Powering down...")
    else:    
        print("Command: Start analysing...")
        print("Jarvis: At your service, sir")
        print("\n")
        
        todatetime = lambda x: datetime.datetime.strptime(x,"%Y-%m-%d")
        data.loc[:,'date'] = data['date'].apply(todatetime)  #convert date from str to datetime object
        data = data.rename(index=data['name']) #rename index 
        today = np.sort(data.date.unique())[-1]
        yday = np.sort(data.date.unique())[-2]
        
        
        ############### Rocket up / down  #######################################################
        dtoday = data.loc[data['date']==today]
        dtoday.insert(loc=len(dtoday.columns),column='prevclose',value=data.loc[data['date']==yday].close)   #insert yday close as new column    
        dtoday = dtoday.drop(columns=['name','date','open','low','high']) #drop adjusted close price and date
        dtoday = dtoday[['volume','close','prevclose']]
        dtoday.insert(loc=len(dtoday.columns),column='% change',value=(dtoday.close/dtoday.prevclose-1)*100)   #insert % change)    
        dtoday1 = dtoday.loc[dtoday.volume * dtoday.close > value_fil] #low transaction value filter
        
        print("************Today's catch*************************")
        print("___________ Rocket up > 5% _______________________")
        d2up = dtoday1.loc[dtoday1['% change'] > 5].sort_values(by=['% change'],ascending=False)
        print(d2up.to_string())
        print("___________ Crash down <-5% ______________________")
        d2down = dtoday1.loc[dtoday1['% change'] < -5].sort_values(by=['% change'])
        print(d2down.to_string())
        
        ############### Volume spike  #######################################################
        nvol = 90
        dfvol = data.loc[data['date'] >= np.sort(data.date.unique())[-1*nvol]]
        dfvolavg = dfvol.groupby(['name']).mean()  #get average volume
        dtodayvol = dtoday[['volume']]                         
        dtodayvol = dtodayvol.rename(columns={'volume':'2dayvol'}) 
        volfinal = pd.merge(dfvolavg,dtodayvol,left_index=True,right_index=True)
        volfinal['% increase'] = (volfinal['2dayvol']/volfinal['volume']-1)*100
        volfinal = volfinal.loc[volfinal['% increase']>50]
        volfinal = volfinal[['volume', '2dayvol', '% increase']].sort_values(by=['% increase'],ascending=False)
        print("___________ Spike over "+str(nvol)+"-day Volumes ___________")
        print(volfinal.to_string())
        print("\n")
        
        ############### Continuous increase in closing price ###################################
        d = np.sort(data.date.unique())[-5:][::-1] #last 5 days sorted desc
        data5 = data.loc[data.date.isin(d)]    #get 5 days record
        
        l2,l3,l4,l5=pd.DataFrame(),pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
        for keys in stocklist:
            check = round(data5.loc[(data5.name==keys)&(data5.date==d[0])].close.values[0],3) > (
                    round(data5.loc[(data5.name==keys)&(data5.date==d[1])].close.values[0],3))
            if check:
                temp1 = data5.loc[(data5.name==keys)&(data5.date==d[1])][['close']]
                temp = data5.loc[(data5.name==keys)&(data5.date==d[0])][['close']]
                l2 = l2.append(pd.concat([temp1,temp],axis=1))
                
        for keys in l2.index:
            check = round(data5.loc[(data5.name==keys)&(data5.date==d[1])].close.values[0],3) > (
                    round(data5.loc[(data5.name==keys)&(data5.date==d[2])].close.values[0],3))
            if check:
                temp2 = data5.loc[(data5.name==keys)&(data5.date==d[2])][['close']]
                temp1 = data5.loc[(data5.name==keys)&(data5.date==d[1])][['close']]
                temp = data5.loc[(data5.name==keys)&(data5.date==d[0])][['close']]                 
                l3 = l3.append(pd.concat([temp2,temp1,temp],axis=1))
                
        for keys in l3.index:
            check = round(data5.loc[(data5.name==keys)&(data5.date==d[2])].close.values[0],3) > (
                    round(data5.loc[(data5.name==keys)&(data5.date==d[3])].close.values[0],3))
            if check:
                temp3 = data5.loc[(data5.name==keys)&(data5.date==d[3])][['close']]
                temp2 = data5.loc[(data5.name==keys)&(data5.date==d[2])][['close']]
                temp1 = data5.loc[(data5.name==keys)&(data5.date==d[1])][['close']]
                temp = data5.loc[(data5.name==keys)&(data5.date==d[0])][['close']]                 
                l4 = l4.append(pd.concat([temp3,temp2,temp1,temp],axis=1))
        
        for keys in l4.index:
            check = round(data5.loc[(data5.name==keys)&(data5.date==d[3])].close.values[0],3) > (
                    round(data5.loc[(data5.name==keys)&(data5.date==d[4])].close.values[0],3))
            if check:
                temp4 = data5.loc[(data5.name==keys)&(data5.date==d[4])][['close']]
                temp3 = data5.loc[(data5.name==keys)&(data5.date==d[3])][['close']]
                temp2 = data5.loc[(data5.name==keys)&(data5.date==d[2])][['close']]
                temp1 = data5.loc[(data5.name==keys)&(data5.date==d[1])][['close']]
                temp = data5.loc[(data5.name==keys)&(data5.date==d[0])][['close']]                 
                l5 = l5.append(pd.concat([temp4,temp3,temp2,temp1,temp],axis=1))
                    
        #Remove l3 from l2, l4 from l3, l5 from l4
        l2 = l2.filter(items=l2.index.difference(l3.index),axis='index')        
        l3 = l3.filter(items=l3.index.difference(l4.index),axis='index')        
        l4 = l4.filter(items=l4.index.difference(l5.index),axis='index')            
        print("*********************Weekly's catch*****************************")
        print("--------------Continuous increase in closing price--------------")
        print("____________________________ 4 days ____________________________")
        print(l5.to_string())
        print("____________________________ 3 days ____________________________")
        print(l4.to_string())
        print("____________________________ 2 days ____________________________")
        print(l3.to_string())
        print("____________________________ 1 day _____________________________")
        print(l2.to_string())
        print("\n")
        
        ############### Continuous decrease in closing price ###################################
        n2,n3,n4,n5=pd.DataFrame(),pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
        for keys in stocklist:
            check = round(data5.loc[(data5.name==keys)&(data5.date==d[0])].close.values[0],3) < (
                    round(data5.loc[(data5.name==keys)&(data5.date==d[1])].close.values[0],3))
            if check:
                temp1 = data5.loc[(data5.name==keys)&(data5.date==d[1])][['close']]
                temp = data5.loc[(data5.name==keys)&(data5.date==d[0])][['close']]
                n2 = n2.append(pd.concat([temp1,temp],axis=1))
                
        for keys in n2.index:
            check = round(data5.loc[(data5.name==keys)&(data5.date==d[1])].close.values[0],3) < (
                    round(data5.loc[(data5.name==keys)&(data5.date==d[2])].close.values[0],3))
            if check:
                temp2 = data5.loc[(data5.name==keys)&(data5.date==d[2])][['close']]
                temp1 = data5.loc[(data5.name==keys)&(data5.date==d[1])][['close']]
                temp = data5.loc[(data5.name==keys)&(data5.date==d[0])][['close']]                 
                n3 = n3.append(pd.concat([temp2,temp1,temp],axis=1))
                
        for keys in n3.index:
            check = round(data5.loc[(data5.name==keys)&(data5.date==d[2])].close.values[0],3) < (
                    round(data5.loc[(data5.name==keys)&(data5.date==d[3])].close.values[0],3))
            if check:
                temp3 = data5.loc[(data5.name==keys)&(data5.date==d[3])][['close']]
                temp2 = data5.loc[(data5.name==keys)&(data5.date==d[2])][['close']]
                temp1 = data5.loc[(data5.name==keys)&(data5.date==d[1])][['close']]
                temp = data5.loc[(data5.name==keys)&(data5.date==d[0])][['close']]                 
                n4 = n4.append(pd.concat([temp3,temp2,temp1,temp],axis=1))
        
        for keys in n4.index:
            check = round(data5.loc[(data5.name==keys)&(data5.date==d[3])].close.values[0],3) < (
                    round(data5.loc[(data5.name==keys)&(data5.date==d[4])].close.values[0],3))
            if check:
                temp4 = data5.loc[(data5.name==keys)&(data5.date==d[4])][['close']]
                temp3 = data5.loc[(data5.name==keys)&(data5.date==d[3])][['close']]
                temp2 = data5.loc[(data5.name==keys)&(data5.date==d[2])][['close']]
                temp1 = data5.loc[(data5.name==keys)&(data5.date==d[1])][['close']]
                temp = data5.loc[(data5.name==keys)&(data5.date==d[0])][['close']]                 
                n5 = n5.append(pd.concat([temp4,temp3,temp2,temp1,temp],axis=1))
                
        #Remove n3 from n2, n4 from n3, n5 from n4
        n2 = n2.filter(items=n2.index.difference(n3.index),axis='index')        
        n3 = n3.filter(items=n3.index.difference(n4.index),axis='index')        
        n4 = n4.filter(items=n4.index.difference(n5.index),axis='index')     
        
        print("--------------Continuous decrease in closing price--------------")
        print("____________________________ 4 days ____________________________")
        print(n5.to_string())
        print("____________________________ 3 days ____________________________")
        print(n4.to_string())
        print("____________________________ 2 days ____________________________")
        print(n3.to_string())
        print("____________________________ 1 day _____________________________")
        print(n2.to_string())
        print("\n")
                
        ############### Bumpy rides (based on transacted, not close) #####################################
        
        filter = data5.groupby(['name']).mean() #get average  
        filter = filter.loc[filter.volume * filter.close > value_fil] #low transaction value filter
        data5a = data5.loc[data5.name.isin(filter.index)]   #filter out raw records with low value filter    
                                   
        roller = pd.DataFrame()
        roller.insert(loc=len(roller.columns),column='fivedaylow',value=(data5a.groupby(['name']).min().low))
        roller.insert(loc=len(roller.columns),column='fivedayhigh',value=(data5a.groupby(['name']).max().high))
        roller.insert(loc=len(roller.columns),column='% change',value=(((roller.fivedayhigh/roller.fivedaylow)-1)*100))
        roller_f = roller.loc[roller['% change']>20].sort_values(by=['% change'],ascending=False)
        print("---------Bumpy rides, week high - low > 20% (based on high/low, not close)--------------")    
        print(roller_f.to_string())
        
        ############### Moving averaging  #######################################################
     
        dtodayclose = dtoday[['close']] #select only 'close' and return as dataframe
        dtodayclose = dtodayclose.rename(columns={'close':'2dayclose'})
        
        ######### Trending - Moving Average #######################################################
        dfma,dfmad = pd.DataFrame(),pd.DataFrame()
        for i, n in enumerate(ma):
            d1data = data.loc[data['date'] >= np.sort(data.date.unique())[-1*n]]  #get records of last number of trading days
            d1dataavg = d1data.groupby(['name']).mean() #get average
            d1dataavg = d1dataavg.loc[d1dataavg.volume * d1dataavg.close > value_fil] #low transaction value filter
            final = pd.merge(d1dataavg,dtodayclose,left_index=True,right_index=True)
            final = final.reset_index() # reset index and move index to column     
          
            finalup=final.loc[final['2dayclose']>final['close']][['index','2dayclose','close','volume']] #get ma close price and volume
            finalup=finalup.rename(columns={'close':str(n)+"_MA",'volume':str(n)+"_vol"})  #and rename
            
            finald=final.loc[final['2dayclose']<final['close']][['index','2dayclose','close','volume']] 
            finald=finald.rename(columns={'close':str(n)+"_MA",'volume':str(n)+"_vol"})  #and rename
            
            if i < 1:
                dfma=dfma.append(finalup) 
                dfmad=dfmad.append(finald)
            else:
                dfma=dfma.merge(finalup,how='outer')
                dfmad=dfmad.merge(finald,how='outer')
                
        temp = ['index','2dayclose']
        temp.extend([str(x)+"_MA" for x in ma])
        txt3MA = [str(x)+"_MA" for x in ma[:3]]    
        print("\n")
        print("*****************************Long catch**************************************")
        print("-------------------------Uptrending - Above MA-------------------------------")
        print(dfma.loc[:,temp].dropna(subset=txt3MA).to_string())    #filter off rows with NaN in first 3 MA
        print("\n")
        
        print("------------------------Downtrending - Below MA------------------------------")
        print(dfmad.loc[:,temp].dropna(subset=txt3MA).to_string())   
        print("\n")
            
        ############### High-low ###################################
        
        dfhl = pd.DataFrame()
        for keys in stocklist:
            a = data.loc[data['name']==keys]
            a5 = a.loc[a.date.isin(d)]    #get last five 5-day records
            
            if dtodayclose.loc[keys][0] <= a['close'].min() :  #all-time low
                n_dl = 7000
            else:
                #dl = a.loc[a['close'] < dtodayclose.loc[keys].values[0]].iloc[-1,:].date   
                dl = a.loc[a['close']< a5.close.max()].iloc[-1,:].date     #use one-week high instead of today's close to compare, to filter out small movements
                n_dl = (datetime.datetime.today() - dl).days
            
            if dtodayclose.loc[keys][0] >= a['close'].max() :  #all-time high
                n_dh = 7000            
            else:
                #dh = a.loc[a['close'] > dtodayclose.loc[keys].values[0]].iloc[-1,:].date    
                dh = a.loc[a['close'] > a5.close.min()].iloc[-1,:].date    #same, use one-week low to compare
                n_dh = (datetime.datetime.today() - dh).days
            
            #Logic to decide high/low
            if n_dl > n_dh:
                n_dh = np.nan
            else:
                n_dl = np.nan
    
            s = pd.Series([n_dh,n_dl],index=['n-day high','n-day low'])
            df = pd.DataFrame(data=s,columns=[keys])
            dfhl = dfhl.append(df.T)
        
        print("******* N-day high & N-day low stocks (based on 5-day high&low) *******")
        print(dfhl.loc[dfhl['n-day high']>20].sort_values(by=['n-day high'],ascending=False).to_string())
        print(dfhl.loc[dfhl['n-day low']>20].sort_values(by=['n-day low'],ascending=False).to_string())
        print("\n")
        
    #    Plot graphs of a stock
    #    stock = data.loc[data['name']=='SUNPOWER']
    #    plt.plot(range(0,len(stock['close'])),stock['close'])
        
        ############### High-low % based on historial max & min ###################################
        print("***************** High-low based on historical max & min **************")
        print("Oldest record date is "+firstrecord)
        #get historical max & min
        dtoday.insert(loc=len(dtoday.columns),column='pmax',value=(data.groupby(['name']).max().close))
        dtoday.insert(loc=len(dtoday.columns),column='pmin',value=(data.groupby(['name']).min().close))
        #compute %
        dtoday.insert(loc=len(dtoday.columns),column='price %',value=((dtoday.close-dtoday.pmin)/(dtoday.pmax-dtoday.pmin)*100))
        dtoday = dtoday.drop(columns=['volume','prevclose','% change',]) #drop adjusted close price and date
        print(dtoday.sort_values(by=['price %'],ascending=False).to_string())
        print("\n")
        
        print("Jarvis: Test complete. Preparing to begin sentiment diagnostics...")
        d90 = np.sort(data.date.unique())[-90:] #last 90 days 
        data90 = data.loc[data.date.isin(d90)]    
        y2016 = pd.date_range(start='1/1/2016', periods=365)
        data2016 = data.loc[data.date.isin(y2016)]    
        y2017 = pd.date_range(start='1/1/2017', periods=365)    
        data2017 = data.loc[data.date.isin(y2017)]    
        print("No of stocks analysed: "+ str(len(data.name.unique())))
        print("# 2 green days or more vs # 2 red days or more    : " + str(len(l5)+len(l4)+len(l3)) + " --- " + str(len(n5)+len(n4)+len(n3)))
        print("# uptrending stocks    vs # downtrending stocks   : " + str(len(dfma.loc[:,temp].dropna(subset=txt3MA))) + " --- " + str(len(dfmad.loc[:,temp].dropna(subset=txt3MA))))
        print("# >20day high stocks   vs # >20day low stocks     : " + str(len(dfhl.loc[dfhl['n-day high']>20])) + " --- " + str(len(dfhl.loc[dfhl['n-day low']>20])))
        print("# >80% hist price      vs # <20% hist price       : " + str(len(dtoday.loc[dtoday['price %']>80])) + " --- " + str(len(dtoday.loc[dtoday['price %']<20])))
        print("Avg transacted value (mil)...")
        print("last 5 days      ： "+ str(round((data5.volume*data5.close).sum()/1E6/5)) +" | # of stocks:" + str(len(data5.name.unique())))
        print("last 90 days     ： "+ str(round((data90.volume*data90.close).sum()/1E6/90)) + " | # of stocks:" + str(len(data90.name.unique())))
        print("year 2017        : "+ str(round((data2017.volume*data2017.close).sum()/1E6/len(data2017.date.unique()))) + " | # of stocks:" + str(len(data2017.name.unique())))
        print("year 2016        : "+ str(round((data2016.volume*data2016.close).sum()/1E6/len(data2016.date.unique()))) + " | # of stocks:" + str(len(data2016.name.unique())))
        print("\n")
        print("Jarvis: Level 1 diganostics completed. Your turn sir. Good huat to you sir.")
        print("Powering down...")
