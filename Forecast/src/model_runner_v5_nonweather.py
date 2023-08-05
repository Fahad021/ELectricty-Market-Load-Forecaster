#!/usr/bin/env python
# coding: utf-8


#libraries

import pandas as pd
import numpy  as np 
import pickle
import joblib
import http.client
import json
import time
import csv
from   datetime import datetime, timedelta
import holidays
import sqlite3
import re
import sys
if sys.version_info[0] < 3: 
    from StringIO import StringIO
else:
    from io import StringIO

# Configure Nrgstrem API call functionalities

class NRGStreamApi:    
    
    def __init__(self,username=None,password=None):
        self.username = 'Fahad'
        self.password = 'ABFAHc2'                
        self.server = 'api.nrgstream.com'        
        self.tokenPath = '/api/security/token'
        self.releasePath = '/api/ReleaseToken'
        self.tokenPayload = f'grant_type=password&username={self.username}&password={self.password}'
        self.tokenExpiry = datetime.now() - timedelta(seconds=60)
        self.accessToken = ""        
        
    def getToken(self):
        try:
            if self.isTokenValid() == False:                     
                headers = { }
                # Connect to API server to get a token
                conn = http.client.HTTPSConnection(self.server)
                conn.request('POST', self.tokenPath, self.tokenPayload, headers)
                res = conn.getresponse()
                res_code = res.code
                res_data = res.read()
                # Check if the response is good

                if res_code == 200:
                    # Decode the token into an object
                    jsonData = json.loads(res_data.decode('utf-8'))
                    self.accessToken = jsonData['access_token']
                    # Calculate new expiry date
                    self.tokenExpiry = datetime.now() + timedelta(seconds=jsonData['expires_in'])                        
                                #print('token obtained')
                                #print(self.accessToken)
                else:
                    print(res_data.decode('utf-8'))
                conn.close()
        except Exception as e:
            print(f"getToken: {str(e)}")
            # Release token if an error occured
            self.releaseToken()      

    def releaseToken(self):
        try:        
            headers = {'Authorization': f'Bearer {self.accessToken}'}
            conn = http.client.HTTPSConnection(self.server)
            conn.request('DELETE', self.releasePath, None, headers)
            res = conn.getresponse()
            res_code = res.code
            if res_code == 200:   
                # Set expiration date back to guarantee isTokenValid() returns false                
                self.tokenExpiry = datetime.now() - timedelta(seconds=60)
                #print('token released')            
        except Exception as e:
            print(f"releaseToken: {str(e)}")
                    
    def isTokenValid(self):
        if self.tokenExpiry is None:
            return False
        elif datetime.now() >= self.tokenExpiry:            
            return False
        else:
            return True            
    
    def GetStreamDataByStreamId(self,streamIds, fromDate, toDate, dataFormat='csv', dataOption=''):
        stream_data = ""
        # Set file format to csv or json
        DataFormats = {'csv': 'text/csv', 'json': 'Application/json'}
        try:                        
            for streamId in streamIds:    
                # Get an access token            
                self.getToken()
                if self.isTokenValid():
                    # Setup the path for data request. Pass dates in via function call
                    path = f'/api/StreamData/{streamId}'
                    if fromDate != '' and toDate != '':
                        path += f'?fromDate={fromDate.replace(" ", "%20")}&toDate={toDate.replace(" ", "%20")}'
                    if dataOption != '':
                        if fromDate != '' and toDate != '':
                            path += f'&dataOption={dataOption}'        
                        else:
                            path += f'?dataOption={dataOption}'        

                    # Create request header
                    headers = {
                        'Accept': DataFormats[dataFormat],
                        'Authorization': f'Bearer {self.accessToken}',
                    }
                    # Connect to API server
                    conn = http.client.HTTPSConnection(self.server)
                    conn.request('GET', path, None, headers)
                    res = conn.getresponse()
                    res_code = res.code
                    if res_code == 200:   
                        try:
                            print(f'{datetime.now()} Outputing stream {path} res code {res_code}')
                            # output return data to a text file            
                            if dataFormat == 'csv':
                                stream_data += res.read().decode('utf-8').replace('\r\n','\n') 
                            elif dataFormat == 'json':
                                stream_data += json.dumps(json.loads(res.read().decode('utf-8')), indent=2, sort_keys=False)
                            conn.close()

                        except Exception as e:
                            print(e)
                            self.releaseToken()
                            return None
                    else:
                        print(
                            f"{str(res_code)} - {str(res.reason)} - "
                            + str(res.read().decode('utf-8'))
                        )

                self.releaseToken()
                # Wait 1 second before next request
                time.sleep(1)
            return stream_data
        except Exception as e:
            print(e)
            self.releaseToken()
            return None
        
        
    def StreamDataOptions(self, streamId, dataFormat='csv'):
        try:  
            DataFormats = {'csv': 'text/csv', 'json': 'Application/json'}
            resultSet = {}
            for streamId in streamIds:
                # Get an access token
                if streamId not in resultSet:
                    self.getToken()
                    if self.isTokenValid(): 
                        # Setup the path for data request.
                        path = f'/api/StreamDataOptions/{streamId}'
                        # Create request header
                        headers = {
                            'Accept': DataFormats[dataFormat],
                            'Authorization': f'Bearer {self.accessToken}',
                        }
                        # Connect to API server
                        conn = http.client.HTTPSConnection(self.server)
                        conn.request('GET', path, None, headers)
                        res = conn.getresponse()
                        self.releaseToken()
                        if dataFormat == 'csv':
                            resultSet[streamId] = res.read().decode('utf-8').replace('\r\n','\n') 
                        elif dataFormat == 'json':
                            resultSet[streamId] = json.dumps(json.loads(res.read().decode('utf-8')), indent=2, sort_keys=False)
                    time.sleep(1)
            return resultSet
        except Exception as e:
            print(e)
            self.releaseToken()
            return None          

        except Exception as e:            
            self.releaseToken()                        
            return str(e)        

# Authenticate with your NRG Stream username and password    
nrgStreamApi = NRGStreamApi('Username','Password')      

# in-memory weather database loading- deleted

#conn = sqlite3.connect('weather_db.db')
#c = conn.cursor()
#c.execute('''  
#SELECT * FROM HISTORICALFCAST
#          ''')
#results = c.fetchall()
#weather_df = pd.DataFrame(results)

#c.close()
#conn.close()

# generate time-stamps for forecast hours

now_time                 = datetime.now()
year_of_run              = now_time.year
month_of_run             = now_time.month
day_of_run               = now_time.day
forecast_start           = datetime(year_of_run ,month_of_run , day_of_run)
forecast_interval_stamps = pd.date_range(forecast_start, periods=24, freq= 'h')

# create the dataframe to hold input data for 24-h forecast

column_list = [
       'hour_of_day', 'off_peak', 'on_peak', 'day',
       'sin.day', 'cos.day',     'sin.hour', 'cos.hour', 'weekend', 'monday',
       'tuesday', 'wednesday',   'thursday', 'friday', 'saturday', 'sunday',
       'month_0', 'month_1',     'month_2', 'month_3', 'month_4', 'month_5',
       'month_6', 'month_7',     'month_8', 'month_9', 'month_10', 'month_11',
       'hour_0',  'hour_1',      'hour_2', 'hour_3', 'hour_4', 'hour_5', 'hour_6',
       'hour_7',  'hour_8',      'hour_9', 'hour_10', 'hour_11', 'hour_12',
       'hour_13', 'hour_14',     'hour_15', 'hour_16', 'hour_17', 'hour_18',
       'hour_19', 'hour_20', 'hour_21', 'hour_22', 'hour_23', 'year',
       'sunlight_avaialbility', 'AIL_previous_hour', 'AIL_24h_lagged',
       'AIL_oneweek_lagged', 'holiday', 'monday_holiday', 'tuesday_holiday',
       'wednesday_holiday', 'thursday_holiday', 'friday_holiday',
       'weekend_holiday', 'month0_mondayholiday', 'month1_mondayholiday',
       'month2_mondayholiday', 'month3_mondayholiday', 'month4_mondayholiday',
       'month5_mondayholiday', 'month6_mondayholiday', 'month7_mondayholiday',
       'month8_mondayholiday', 'month9_mondayholiday', 'month10_mondayholiday',
       'month11_mondayholiday', 'month0_fridayholiday', 'month1_fridayholiday',
       'month2_fridayholiday', 'month3_fridayholiday', 'month4_fridayholiday',
       'month5_fridayholiday', 'month6_fridayholiday', 'month7_fridayholiday',
       'month8_fridayholiday', 'month9_fridayholiday', 'month10_fridayholiday',
       'month11_fridayholiday', 'sunlight_mondayholiday',
       'sunlight_tuesdayholiday' 
]

prediction_df            = pd.DataFrame(columns = column_list)


###########################################################################
####################### Feature Generation ################################
###########################################################################

hrs                          = np.arange(0, 24, 1).tolist()
prediction_df['hour_of_day'] = hrs

# off-peak and on-peak hour assignment 
conditions = [
    (prediction_df['hour_of_day'] < 7),
    (prediction_df['hour_of_day'] >= 7) & (prediction_df['hour_of_day'] <= 19),
    (prediction_df['hour_of_day'] > 19)
    ]

# create a list of the values we want to assign for each condition
values = [0, 1, 0]

# create a new column and use np.select to assign values to it using our lists as arguments
prediction_df['on_peak'] = np.select(conditions, values)

conditions = [
    (prediction_df['hour_of_day'] < 7),
    (prediction_df['hour_of_day'] >= 7) & (prediction_df['hour_of_day'] <= 19),
    (prediction_df['hour_of_day'] > 19)
    ]

values = [1, 0, 1]
prediction_df['off_peak'] = np.select(conditions, values)



# add time-related features

prediction_df['day']      = forecast_interval_stamps.day
prediction_df['sin.day']  = np.sin(prediction_df['day']*2*np.pi/365 + np.pi/4)
prediction_df['cos.day']  = np.cos(prediction_df['day']*2*np.pi/365 + np.pi/4)
prediction_df['sin.hour'] = np.sin(prediction_df['hour_of_day']*2*np.pi/24)
prediction_df['cos.hour'] = np.cos(prediction_df['hour_of_day']*2*np.pi/24)
weekdays                  = [d.weekday() for d in forecast_interval_stamps]
prediction_df['weekend']  = [1 if d >= 5 else 0 for d in weekdays]



name_of_day = forecast_start.strftime("%A")
for i in range(9,9+7):
    if name_of_day.lower() == prediction_df.columns[i]:
        prediction_df.iloc[:,i] = 1
    else:
        prediction_df.iloc[:,i] = 0

#prediction_df.iloc[:,9:9+7]


number_of_month = forecast_start.strftime("%m")
for i in range(16,16+12):
    if ((int(number_of_month)-1)+16) == i:
        prediction_df.iloc[:,i] = 1
    else:
        prediction_df.iloc[:,i] = 0

#prediction_df.iloc[:, 16:16+12]



hr_matrix = np.identity(24)
prediction_df.iloc[:,28:28+24] = hr_matrix.astype(np.int64)
prediction_df.iloc[:,28:28+24] = prediction_df.iloc[:,28:28+24].astype('int64')


prediction_df.iloc[:,52] = year_of_run



# Leap Year Check
is_leapyear = int(year_of_run) % 4 == 0 and int(year_of_run) % 100 != 0
xls = pd.ExcelFile('edmonton_sunrise_sunset.xls')
dfess_2015 = pd.read_excel(xls, '2015') #df= dafaframe, e= edmonston, ss= sunrise and sunset 
dfess_2016 = pd.read_excel(xls, '2016')
dfess_2015 = dfess_2015[['Sunrise_hr', 'Sunset_hr']]
dfess_2016 = dfess_2016[['Sunrise_hr', 'Sunset_hr']]

for i in range(0,prediction_df.shape[0]):
    if is_leapyear == True:
        criteria = prediction_df.iloc[i,3] #day_of_year
        sunrise  = dfess_2016.iloc[criteria-1,0] #sunrise
        sunset   = dfess_2016.iloc[criteria-1,1] #sunset
        if (prediction_df.iloc[i,0]>= sunrise) and (prediction_df.iloc[i,0] <= sunset):
            prediction_df.iloc[i,53] = 1
        else:
            prediction_df.iloc[i,53] = 0   
    else:
        criteria = prediction_df.iloc[i,3] #day_of_year
        sunrise  = dfess_2015.iloc[criteria-1,0]#sunrise
        sunset   = dfess_2015.iloc[criteria-1,1] #sunset
        if (prediction_df.iloc[i,0]>= sunrise) and (prediction_df.iloc[i,0] <= sunset):
            prediction_df.iloc[i,53] = 1
        else:
            prediction_df.iloc[i,53] = 0

prediction_df.iloc[:,53] = prediction_df.iloc[:,53].astype('int64')
#prediction_df.iloc[:,52]



#one-day in the past feature
datetime_pastday                = now_time + pd.Timedelta(days= -1)
fromDate                        = '{}/{}/{}'.format(datetime_pastday.month,datetime_pastday.day, datetime_pastday.year )
toDate                          = fromDate
stream_data                     = nrgStreamApi.GetStreamDataByStreamId([3], fromDate, toDate, 'csv', '')        
STREAM_DATA                     = StringIO(stream_data)
stream_df_yesterday             = pd.read_csv(STREAM_DATA, sep=";")
stream_df_yesterday             = stream_df_yesterday.iloc[14:38,:]
stream_df_yesterday.columns     = ["Datetime,AIL"]
temp_df                         = stream_df_yesterday['Datetime,AIL'].str.split(",", n = 2, expand = True) 
stream_df_yesterday["Datetime"] = temp_df[0] 
stream_df_yesterday['AIL']      = temp_df[1] 
stream_df_yesterday['AIL']      = pd.to_numeric(stream_df_yesterday['AIL'],errors='coerce')
stream_df_yesterday             = stream_df_yesterday.drop(columns=['Datetime,AIL','Datetime'],axis=1)
prediction_df['AIL_24h_lagged'] = stream_df_yesterday.values
prediction_df['AIL_24h_lagged'] = prediction_df['AIL_24h_lagged'].astype('float64')



# 7-day in the past feature
datetime_pastweek               = now_time + pd.Timedelta(days= -7)
fromDate                        = '{}/{}/{}'.format(datetime_pastweek.month ,datetime_pastweek.day, datetime_pastweek.year)
toDate                          = fromDate
stream_data                     = nrgStreamApi.GetStreamDataByStreamId([3], fromDate, toDate, 'csv', '')        
STREAM_DATA                     = StringIO(stream_data)
stream_df_7daypast              = pd.read_csv(STREAM_DATA, sep=";")
stream_df_7daypast              = stream_df_7daypast.iloc[14:38,:]
stream_df_7daypast.columns      = ["Datetime,AIL"]
temp_df                         = stream_df_7daypast['Datetime,AIL'].str.split(",", n = 2, expand = True) 
stream_df_7daypast["Datetime"]  = temp_df[0] 
stream_df_7daypast['AIL']       = temp_df[1] 
stream_df_7daypast['AIL']       = pd.to_numeric(stream_df_7daypast['AIL'],errors='coerce')
stream_df_7daypast              = stream_df_7daypast.drop(columns=['Datetime,AIL','Datetime'],axis=1)
prediction_df['AIL_oneweek_lagged'] = stream_df_7daypast.values
prediction_df['AIL_oneweek_lagged'] = prediction_df['AIL_oneweek_lagged'].astype('float64')





# holiday features

hl_list = holidays.CA(years=[year_of_run], prov = 'AB').items()
df_hl   = pd.DataFrame(hl_list)
df_hl.columns = ['date','title']
df_hl['date'] = pd.to_datetime(df_hl.date)
if forecast_start.date in list(df_hl['date'].values)==True:
    prediction_df['holiday'] = 1
else:
    prediction_df['holiday'] = 0


prediction_df['monday_holiday']    = prediction_df['monday']    * prediction_df['holiday']
prediction_df['tuesday_holiday']   = prediction_df['tuesday']   * prediction_df['holiday'] 
prediction_df['wednesday_holiday'] = prediction_df['wednesday'] * prediction_df['holiday']
prediction_df['thursday_holiday']  = prediction_df['thursday']  * prediction_df['holiday']
prediction_df['friday_holiday']    = prediction_df['friday']    * prediction_df['holiday']
prediction_df['weekend_holiday']   = prediction_df['weekend']   * prediction_df['holiday']

prediction_df['month0_mondayholiday'] = prediction_df['month_0']*prediction_df['monday_holiday']
prediction_df['month1_mondayholiday'] = prediction_df['month_1']*prediction_df['monday_holiday']
prediction_df['month2_mondayholiday'] = prediction_df['month_2']*prediction_df['monday_holiday']
prediction_df['month3_mondayholiday'] = prediction_df['month_3']*prediction_df['monday_holiday']
prediction_df['month4_mondayholiday'] = prediction_df['month_4']*prediction_df['monday_holiday']
prediction_df['month5_mondayholiday'] = prediction_df['month_5']*prediction_df['monday_holiday']
prediction_df['month6_mondayholiday'] = prediction_df['month_6']*prediction_df['monday_holiday']
prediction_df['month7_mondayholiday'] = prediction_df['month_7']*prediction_df['monday_holiday']
prediction_df['month8_mondayholiday'] = prediction_df['month_8']*prediction_df['monday_holiday']
prediction_df['month9_mondayholiday'] = prediction_df['month_9']*prediction_df['monday_holiday']
prediction_df['month10_mondayholiday'] = prediction_df['month_10']*prediction_df['monday_holiday']
prediction_df['month11_mondayholiday'] = prediction_df['month_11']*prediction_df['monday_holiday']

prediction_df['month0_fridayholiday'] = prediction_df['month_0']*prediction_df['friday_holiday']
prediction_df['month1_fridayholiday'] = prediction_df['month_1']*prediction_df['friday_holiday']
prediction_df['month2_fridayholiday'] = prediction_df['month_2']*prediction_df['friday_holiday']
prediction_df['month3_fridayholiday'] = prediction_df['month_3']*prediction_df['friday_holiday']
prediction_df['month4_fridayholiday'] = prediction_df['month_4']*prediction_df['friday_holiday']
prediction_df['month5_fridayholiday'] = prediction_df['month_5']*prediction_df['friday_holiday']
prediction_df['month6_fridayholiday'] = prediction_df['month_6']*prediction_df['friday_holiday']
prediction_df['month7_fridayholiday'] = prediction_df['month_7']*prediction_df['friday_holiday']
prediction_df['month8_fridayholiday'] = prediction_df['month_8']*prediction_df['friday_holiday']
prediction_df['month9_fridayholiday'] = prediction_df['month_9']*prediction_df['friday_holiday']
prediction_df['month10_fridayholiday'] = prediction_df['month_10']*prediction_df['friday_holiday']
prediction_df['month11_fridayholiday'] = prediction_df['month_11']*prediction_df['friday_holiday']

prediction_df['sunlight_mondayholiday']  = prediction_df['sunlight_avaialbility']*prediction_df['monday_holiday']
prediction_df['sunlight_tuesdayholiday'] = prediction_df['sunlight_avaialbility']*prediction_df['tuesday_holiday']


prediction_df.to_csv('input_data_{}_{}_{}_generate_date_{}.csv'.format(day_of_run, month_of_run, year_of_run, now_time))


#Generate the forecast

loaded_model = joblib.load('model_generated_on_12_2_2021.joblib.dat')

forecast = pd.DataFrame() # this dataframe will contain the forecasts (timestamp and values)
forecast['datetime_of_forecast']= forecast_interval_stamps.values 
forecast['values']              = 0 #initializing the column with zero

prediction_df.iloc[0,prediction_df.columns.get_loc('AIL_previous_hour')] = prediction_df.iloc[23,prediction_df.columns.get_loc('AIL_24h_lagged')] # assignment of AIL_previous_hour (24th hour of previous day)
input_data               = prediction_df.iloc[0] # preparing input data
predictions              = loaded_model.predict(np.array(input_data).reshape((1,-1))) # temporary location for forecast value
prediction_df.iloc[1,prediction_df.columns.get_loc('AIL_previous_hour')] = predictions[0] #load forecast in AIL_previous_hour
forecast.iloc[0,1]       = predictions[0]

for i in range(1, 24):
    input_data          = prediction_df.iloc[i]
    predictions         = loaded_model.predict(np.array(input_data).reshape((1,-1)))
    forecast.iloc[i,1]  = predictions[0]
    if i+1 <= 23:
        prediction_df.iloc[i+1,prediction_df.columns.get_loc('AIL_previous_hour')] = predictions[0]


forecast.to_csv('forecast_{}_{}_{}_generate_date_{}.csv'.format(day_of_run, month_of_run, year_of_run, now_time ))




