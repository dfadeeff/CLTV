#Load libraries
import pandas as pd
import numpy as np
import lifetimes
import calendar
from datetime import datetime

#Load data
data = pd.read_csv("C:\PowerBI-Share\Python_inputs\orders_data.csv")

#Make a subset
data_s = data[["customer","state","order","createdAt","location"]]


#Add the date, just in case
data_s.is_copy = False #turn off the check for that object
data_s["date"] = pd.to_datetime(data_s["createdAt"]).dt.date #that parses the date part
data_s['date'] = pd.to_datetime(data_s['date'])

#Add is_valid column
data_s["isvalid"] = 1
data_s.ix[data_s['state'].isin(['new','canceled','reserved']),['isvalid']] = 0

#Check total # of valid orders
print "Total number of valid orders: ", sum(data_s["isvalid"])
print "Unique customers: ", len(data_s['customer'].unique())
print data_s.shape

#PREPARATION PART
#Aggregate by customer
g = data_s[data_s['isvalid']>0].groupby('customer').apply(lambda x: x['isvalid'].sum()).reset_index()
g.columns = ['customer','validOrders']

# Make a transform with first and last date per customer
p = data_s[data_s['isvalid']>0].groupby('customer')['date'].agg({"first": lambda x: x.min(),"last": lambda x: x.max()}).reset_index()

#Merge the datasets
merged_df = pd.merge(left=g,right=p,how='inner',left_on='customer',right_on='customer',copy=True)
#print(merged_df.shape)
#print(merged_df['validOrders'].sum())

#Deliver frequency, recency, T
#Frequency
merged_df['frequency'] = merged_df['validOrders'] - 1

#Get recency: duration btw first and last purchase
merged_df['recency'] = (merged_df['last'] - merged_df['first']).astype('timedelta64[D]') #D for days, W for weeks etc

#Get T
merged_df["T"] = (pd.datetime.now().date() - merged_df['first']).astype('timedelta64[D]')

m = merged_df[['customer','frequency','recency','T']].set_index('customer')
print(m.shape)


#Make a split by city
c = data_s.groupby('customer')['location'].agg({"first_location": lambda x: x.min()}).reset_index()
print c.head()

#MODEL PART
bgf = lifetimes.BetaGeoFitter(penalizer_coef = 0.0)
bgf.fit(m['frequency'], m['recency'], m['T'])
print bgf


#Prediction for number of expected transactions for next period, days to end of current month
now = datetime.now()
delta = calendar.monthrange(now.year, now.month)[1] - now.day

t = delta
m['predicted_purchases'] = bgf.conditional_expected_number_of_purchases_up_to_time(t, m['frequency'], m['recency'], m['T'])

m = m.reset_index(level=None)
merged_m = pd.merge(left=m,right=c,how="left",left_on='customer',right_on='customer',copy=True)
print(merged_m.head())
print merged_m.shape


print merged_m['predicted_purchases'].sum()
merged_m.to_csv("C:\PowerBI-Share\Python_outputs\cltv.csv",encoding="utf-8")
