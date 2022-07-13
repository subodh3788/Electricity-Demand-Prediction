
import requests
import numpy as np
import pandas as pd
from datetime import datetime,timedelta
import json
from meteostat import Point, Daily
import ssl
ssl._create_default_https_context=ssl._create_unverified_context


#get yesterday's date. This is to get historical data upto yesterday
yesterday=datetime.today().date()-timedelta(days=1)
year=yesterday.year
month=yesterday.month
day=yesterday.day



# Set time period
start = datetime(2019, 1, 1)
end = datetime(year,month,day)

# Create Point for location
LongIsland = Point(40.7891, -73.1350)

# Get daily data. Note that I have already imported meteostat: that is where I pull the data from.
data = Daily(LongIsland, start, end)
HistData = data.fetch() #Luckily, this is aready a pandas dataframe



#clean up a little bit
HistData.drop(['tmin','tmax','snow','wdir','wpgt','pres','tsun'],inplace=True,axis=1)
HistData.rename(columns={'tavg':'Temperature','prcp':'rain','wspd':'windSpeed'},inplace=True)
HistData=HistData[['Temperature','windSpeed','rain']]
HistData.rename_axis(('date'),inplace=True)
HistData['Temperature']=(HistData['Temperature']*1.8)+32 #change degree celcius to Fahrenheit
HistData=HistData.round(2)



#Now we want the forecast that starts today. Let's create a dataframe for that
df_forecast=pd.DataFrame()
response=requests.get('https://api.weather.gov/gridpoints/OKX/61,42/forecast').json() #API



#get data from API in a proper and readable format: as a list
weWant=response['properties']['periods']
startTime=[]
date=[]
Temperature=[]
windSpeed=[]
rain=[]
for i in range(len(weWant)):
    date.append(weWant[i]['endTime'])
    Temperature.append(weWant[i]['temperature'])
    windSpeed.append(weWant[i]['windSpeed'])
    rain.append(weWant[i]['detailedForecast'])



#add columns
df_forecast['date']=date
df_forecast['Temperature']=Temperature
df_forecast['wspd']=windSpeed
df_forecast['rain']=rain
df_forecast['date']=pd.to_datetime(df_forecast['date'])# change date to datetime type
df_forecast['date'] = df_forecast['date'].dt.date#only grabs date and not time



# some columns have bunch of text. So we will extract only numbers which is what we want
df_forecast['windSpeed']=df_forecast['wspd'].astype('str').str.extract('(\d+)').astype(int)
df_forecast['rain_copy']=df_forecast['rain'].astype('str').str.extract('(\d+)%')
df_forecast['rain_copy']=df_forecast['rain_copy'].fillna(0).astype(int)



#clean up a bit
df_forecast.drop(['rain'],inplace=True,axis=1)
df_forecast.drop(['wspd'],inplace=True,axis=1)


# In[10]:


df_forecast.rename(columns={'rain_copy':'rain'},inplace=True)
df_forecast.set_index('date',inplace=True)


df_forecast=df_forecast.groupby(['date'])['Temperature','windSpeed','rain'].mean()



#Now another API for electricity data
key='yjoCqBfoiXRmzEehlBC9ysCc33UFWNLmQ7i8Nd36'
url='https://api.eia.gov/v2/electricity/rto/daily-region-sub-ba-data/data/?api_key=yjoCqBfoiXRmzEehlBC9ysCc33UFWNLmQ7i8Nd36&facets[subba][]=ZONK&start=2019-01-01&data[]=value&facets[timezone][]=Eastern'

electricity=requests.get(url).json()



info=electricity['response']['data']
date=[]
power_consumption=[]
for n in range(len(info)):
    date.append(info[n]['period'])
    power_consumption.append(info[n]['value'])



df_demand=pd.DataFrame() #create an empty dataframe
df_demand['date']=date
df_demand['power_consumption']=power_consumption

df_demand['date']=pd.to_datetime(df_demand['date'])



#Put historical and future weather data together 
df2=HistData.append(df_forecast)
df2=df2.reset_index()


#Now add the electricity data
df3=pd.merge(df2,df_demand,on=['date'],how='outer')



#Now we will put these on postgres sql using AWS
import psycopg2
username='yourusername'
password='yourpassword'
dbname='nameofdatabase'
endpoint='hostname'
port=5432



def connect_to_db(host,database,user,password,port):
    try:
        conn=ps.connect(host=endpoint,database=dbname,user=username,password=password,port=port)
    except ps_OperationalError as e:
        raise error
    else:
        print('Connected!')
    return conn


conn=connect_to_db(endpoint,dbname,username,password,port)


#create database Table:
def create_table(curr):
    create_table_command=("""CREATE TABLE IF NOT EXISTS electricity_demand4 
    (date DATE ,Temperature NUMERIC,windSpeed NUMERIC,
    rain NUMERIC,power_consumption NUMERIC)""")
    curr.execute(create_table_command)
    

#Write insert command
def insert_into_table(curr,date,Temperature,windSpeed,rain,power_consumption):
    insert_into_electricity_demand4=("""INSERT INTO electricity_demand4 (date,Temperature,windSpeed,rain,power_consumption) 
    VALUES (%s,%s,%s,%s,%s);""")
    row_to_insert=(date,Temperature,windSpeed,rain,power_consumption)
    curr.execute(insert_into_electricity_demand4, row_to_insert)


def check_if_date_exists(curr,date):
    query=("""SELECT date FROM electricity_demand4 WHERE date=%s""")
    curr.execute(query,(date,))
    return curr.fetchone() is not None


def update_row(curr,date,Temperature,windSpeed,rain,power_consumption):
    query=("""UPDATE electricity_demand4
        SET Temperature=%s,
        windSpeed=%s,
        rain=%s,
        power_consumption=%s
        WHERE date=%s;""")
    vars_to_update=(Temperature,windSpeed,rain,power_consumption,date)
    curr.execute(query,vars_to_update)
    
def update_db(curr,df3):    
    tmp_df=pd.DataFrame(columns=['date','Temperature','windSpeed','rain','power_consumption'])
#Adding data to the database
    for i,row in df3.iterrows():
        if check_if_date_exists(curr,row['date']):
            update_row(curr,row['date'],row['Temperature'],row['windSpeed'],row['rain'],row['power_consumption'])
        else:
            tmp_df=tmp_df.append(row)
    return tmp_df



#for loop to insert data into the table
def append_from_df_to_db(curr,df3):
    for i,row in df3.iterrows():
        insert_into_table(curr,row['date'],row['Temperature'],row['windSpeed'],row['rain'],row['power_consumption'])
    


#connect and create table
conn=connect_to_db(endpoint,dbname,username,password,port)
curr=conn.cursor()
create_table(curr)
conn.commit()


new_date_df=update_db(curr,df3) #saves the new data if date already doesn't exist
conn.commit()


append_from_df_to_db(curr,new_date_df)  #appends the new data(newdatedf) to the original
conn.commit()


conn.close() #close the connection





