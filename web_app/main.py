

import requests
import numpy as np
import pandas as pd
from datetime import datetime,timedelta
import json
from meteostat import Point, Daily
import numpy as np
from flask import Flask, request, jsonify, render_template
import pickle


app = Flask(__name__) #Initialize the flask App
model = pickle.load(open('model.pkl','rb'))

df_forecast=pd.DataFrame()
#API call
response=requests.get('https://api.weather.gov/gridpoints/OKX/61,42/forecast').json()
weWant=response['properties']['periods']
date=[]
Temperature=[]
for i in range(len(weWant)):
    date.append(weWant[i]['endTime'])
    Temperature.append(weWant[i]['temperature'])
df_forecast['date']=date
df_forecast['Temperature']=Temperature
df_forecast['date']=pd.to_datetime(df_forecast['date'])
df_forecast['date'] = df_forecast['date'].dt.date 
df_forecast['date']=pd.to_datetime(df_forecast['date'])
df_forecast.set_index('date',inplace=True)
df_forecast=df_forecast.groupby(['date'])['Temperature'].mean()


@app.route('/', methods=['GET', 'POST'])

def hello_world():
    
    
    if request.method == 'GET':
        return render_template('index.html')
    else:
        x = request.form.get('temperature') or -1000
        output=str(round(model.predict([[x]])[0],2))
        date_i=request.form.get('date') 
        date=datetime.strptime(date_i, '%Y-%m-%d').date() if date_i!='' else 0 
        real_date=df_forecast[date:date].values[0] if date!=0 else -1000
        date_output=str(round(model.predict([[real_date]])[0],2))
        if x==-1000 and real_date==-1000:
            return render_template('index.html',prediction_text="Put something in there!")
        if x!=-1000 and real_date!=-1000:
            return render_template('index.html',prediction_text="Only one please!")
        if x!=-1000 and real_date==-1000:
            return render_template('index.html',prediction_text="Electricity demand is {} MWH".format(output))
        if real_date!=-1000 and x==-1000:
            return render_template('index.html',prediction_text="Electricity demand will be {} MWH".format(date_output))
        
        
    
if __name__ == "__main__":
    app.run(port=5002,debug=True)

    
        
        
