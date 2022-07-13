
import psycopg2
import pandas as pd
from matplotlib import pyplot  as plt
from datetime import datetime
import numpy as np
import sklearn
from sklearn.tree import DecisionTreeRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error as MSE


#connect to database
host='yourhostname'
database='yourdatabasename'
user='username'
password='yourpassword'
port=5432
conn=psycopg2.connect(host=host,database=database,user=user,password=password,port=port)


#get table
curr=conn.cursor()
curr.execute("select * from electricity_demand4")


df=pd.DataFrame(rows,columns=['date','temperature','windspeed','rain','electricity(MWH)'])


df.sort_values(by=['date'],inplace=True,ignore_index=True)
df['temperature']=df['temperature'].astype('float64')
df['windspeed']=df['windspeed'].astype('float64')
df['rain']=df['rain'].astype('float64')
df['electricity(MWH)']=df['electricity(MWH)'].astype('float64')


#scatterplot to see the relation
plt.style.use('seaborn')
plt.scatter(df['temperature'],df['electricity(MWH)'],edgecolor='black',alpha=1,linewidth=1)
plt.title('temperature vs electricity')
plt.xlabel('temperature')
plt.ylabel('electricity')

plt.tight_layout()

plt.show()


#more plots
plt.style.use('seaborn')
plt.scatter(df['rain'],df['electricity(MWH)'],edgecolor='black',alpha=1,linewidth=1)
plt.title('rain vs electricity')
plt.xlabel('rain')
plt.ylabel('electricity')

plt.tight_layout()

plt.show()


#more scatterplots
plt.style.use('seaborn')
plt.scatter(df['windspeed'],df['electricity(MWH)'],edgecolor='black',alpha=1,linewidth=1)
plt.title('windspeed vs electricity')
plt.xlabel('windspeed')
plt.ylabel('electricity')

plt.tight_layout()

plt.show()


#cleanup a bit
df['date']=pd.to_datetime(df['date'])
df_temp=df[df['electricity(MWH)'].notnull()]
df_month_temp=df[['electricity(MWH)']].resample('M').mean().round(2)


#split into train and test
x=df_temp.iloc[:,0:1].values #gets the temperature values only with index as a list but here as 2d matrix(n by 1 matrix)
#0:1 gets the 0th column only because 1 is excluded hence presented as a list of list
y=df_temp.iloc[:,3].values #gets the electrity values only without index as a list
x_train,x_test,y_train,y_test=train_test_split(x,y,test_size=0.2,random_state=0)



tree_regression=DecisionTreeRegressor(random_state=0)
tree_regression.fit(x_train,y_train)


#get predicted values vs test values
y_predicted_reg=tree_regression.predict((x_test))
from sklearn import metrics
r_square=metrics.r2_score(y_test,y_predicted_reg)
print('R-square error with Decision Tree Regressor is:',r_square)


from sklearn.ensemble import RandomForestRegressor
rf=RandomForestRegressor()
rf.fit(x_train,y_train)


rf.score(x_test,y_test)


from sklearn.model_selection import ShuffleSplit
from sklearn.model_selection import cross_val_score


cv = ShuffleSplit(n_splits=10, test_size=0.2, random_state=0)

rsc=cross_val_score(RandomForestRegressor(), x_train, y_train, cv=cv)
rsc.mean()

dsc=cross_val_score(DecisionTreeRegressor(), x_train, y_train, cv=cv)
dsc.mean()


from xgboost import XGBRegressor as xgb

xgbr = xgb(verbosity=0) 
xgbr.fit(x_train, y_train)
xgbr.score(x_test, y_test)  


scores = cross_val_score(xgbr, x_train, y_train,cv=10)
scores.mean()


import pickle
with open('model_pickle','wb') as f:
    pickle.dump(tree_regression,f)

