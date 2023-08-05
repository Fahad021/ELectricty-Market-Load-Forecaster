
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


# create the dataframe to hold input data for 24-h forecast

column_list = ['year', 'day_of_month', 'month',
       'day_of_year', 'hour_of_day', 'hour_of_the_week', 'hour_x_day',
       'sin.hour.daily', 'cos.hour.daily', 'sin.hour.weekly',
       'cos.hour.weekly', 'sin.day.yearly', 'cos.day.yearly', 'weekend',
       'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday',
       'sunday', 'temp_calgary', 'temp_edmonton', 'is_Covid',
       'temp_edmonton_lag1', 'temp_edmonton_lag2', 'temp_edmonton_lag3',
       'temp_edmonton_lag4', 'temp_edmonton_lag5', 'temp_edmonton_lag6',
       'temp_edmonton_lag7', 'temp_edmonton_lag8', 'temp_edmonton_lag9',
       'temp_edmonton_lag10', 'temp_edmonton_lag11', 'temp_edmonton_lag12',
       'temp_edmonton_lag13', 'temp_edmonton_lag14', 'temp_edmonton_lag15',
       'temp_edmonton_lag16', 'temp_edmonton_lag17', 'temp_edmonton_lag18',
       'temp_edmonton_lag19', 'temp_edmonton_lag20', 'temp_edmonton_lag21',
       'temp_edmonton_lag22', 'temp_edmonton_lag23', 'temp_edmonton_lag24',
       'temp_calgary_lag1', 'temp_calgary_lag2', 'temp_calgary_lag3',
       'temp_calgary_lag4', 'temp_calgary_lag5', 'temp_calgary_lag6',
       'temp_calgary_lag7', 'temp_calgary_lag8', 'temp_calgary_lag9',
       'temp_calgary_lag10', 'temp_calgary_lag11', 'temp_calgary_lag12',
       'temp_calgary_lag13', 'temp_calgary_lag14', 'temp_calgary_lag15',
       'temp_calgary_lag16', 'temp_calgary_lag17', 'temp_calgary_lag18',
       'temp_calgary_lag19', 'temp_calgary_lag20', 'temp_calgary_lag21',
       'temp_calgary_lag22', 'temp_calgary_lag23', 'temp_calgary_lag24']


# generate time-stamps for forecast hours

now_time                 = datetime.now()
year_of_run              = now_time.year
month_of_run             = now_time.month
day_of_run               = now_time.day
forecast_start           = datetime(year_of_run ,month_of_run , day_of_run)
forecast_interval_stamps = pd.date_range(forecast_start, periods=24, freq= 'h')

# Generate input dataframe

prediction_df            = pd.DataFrame(columns = column_list)

prediction_df['day_of_month']  = forecast_interval_stamps.day
prediction_df['month']         = forecast_interval_stamps.month

day = pd.Series([d.timetuple().tm_yday for d in forecast_interval_stamps])
prediction_df['day_of_year'] = day

hrs                          = np.arange(0, 24, 1).tolist()
prediction_df['hour_of_day'] = hrs

prediction_df['hour_of_the_week'] = forecast_interval_stamps.dayofweek * 24 + (forecast_interval_stamps.hour + 1)

# add time-related features


prediction_df['hour_x_day'] = prediction_df['day_of_year']*prediction_df['hour_of_day'] 

prediction_df['sin.hour.daily']  = np.sin(prediction_df['hour_of_day']*2*np.pi/24)
prediction_df['cos.hour.daily']  = np.cos(prediction_df['hour_of_day']*2*np.pi/24)

prediction_df['sin.hour.weekly'] = np.sin(prediction_df['hour_of_the_week']*2*np.pi/168 )
prediction_df['cos.hour.weekly'] = np.cos(prediction_df['hour_of_the_week']*2*np.pi/168 )

prediction_df['sin.day.yearly'] = np.sin(prediction_df['day_of_year']*2*np.pi/365 + 3*np.pi/2)
prediction_df['cos.day.yearly'] = np.cos(prediction_df['day_of_year']*2*np.pi/365 + 3*np.pi/2)



weekdays                  = [d.weekday() for d in forecast_interval_stamps]
prediction_df['weekend']  = [1 if d >= 5 else 0 for d in weekdays]

name_of_day = forecast_start.strftime("%A")
for i in range(14,14+7):
    if name_of_day.lower() == prediction_df.columns[i]:
        prediction_df.iloc[:,i] = 1
    else:
        prediction_df.iloc[:,i] = 0

prediction_df['year'] = year_of_run-2000


# weather feature

fromDate = '{}/{}/{}'.format(month_of_run,day_of_run, year_of_run )
toDate   = fromDate

df_clgry   = pd.DataFrame()
df_edmtn   = pd.DataFrame()
df_list = {}

streams = [242498, 242497]
city_names = ['calgary', 'edmonton']
k = 0

for i in streams:
    stream_data = nrgStreamApi.GetStreamDataByStreamId([i], fromDate, toDate, 'csv', '')        
    STREAM_DATA = StringIO(stream_data)
    df = pd.read_csv(STREAM_DATA, sep=";")
    
    df = df[16:(16+24)] # removing header information
    df.columns = ["Datetime,temp,wind,direction"]
    new = df['Datetime,temp,wind,direction'].str.split(",", n = 4, expand = True) 
    # making separate first name column from new data frame 
    df["Datetime"]= new[0] 
    df["temp"]= new[1] 
    df["wind"] = new [2]
    df["direction"] = new[3]
    df = df.drop(['Datetime,temp,wind,direction','direction','Datetime', 'wind'],axis=1)
    df = df.reset_index(drop=True)
    df.columns = [str(col) + '_'+ city_names[k] for col in df.columns]
    df_list[k] = df
    k = k+1

df_clgry   = df_list[0]
df_edmtn   = df_list[1]

alberta_weather_merged = pd.concat([df_clgry, df_edmtn], axis=1)
alberta_weather_merged['temp_calgary'] = alberta_weather_merged['temp_calgary'].astype('float64')
alberta_weather_merged['temp_edmonton'] = alberta_weather_merged['temp_edmonton'].astype('float64')
prediction_df['temp_calgary'] = alberta_weather_merged.iloc[:,0].values
prediction_df['temp_edmonton'] = alberta_weather_merged.iloc[:,1].values

# Covid feature input


prediction_df['is_Covid'] = 0


# Lagged Temperature feature input

prediction_df['temp_edmonton_lag1'] = prediction_df['temp_edmonton'].shift(1)
prediction_df['temp_edmonton_lag2'] = prediction_df['temp_edmonton'].shift(2)
prediction_df['temp_edmonton_lag3'] = prediction_df['temp_edmonton'].shift(3)
prediction_df['temp_edmonton_lag4'] = prediction_df['temp_edmonton'].shift(4)
prediction_df['temp_edmonton_lag5'] = prediction_df['temp_edmonton'].shift(5)
prediction_df['temp_edmonton_lag6'] = prediction_df['temp_edmonton'].shift(6)
prediction_df['temp_edmonton_lag7'] = prediction_df['temp_edmonton'].shift(7)
prediction_df['temp_edmonton_lag8'] = prediction_df['temp_edmonton'].shift(8)
prediction_df['temp_edmonton_lag9'] = prediction_df['temp_edmonton'].shift(9)
prediction_df['temp_edmonton_lag10'] = prediction_df['temp_edmonton'].shift(10)
prediction_df['temp_edmonton_lag11'] = prediction_df['temp_edmonton'].shift(11)
prediction_df['temp_edmonton_lag12'] = prediction_df['temp_edmonton'].shift(12)
prediction_df['temp_edmonton_lag13'] = prediction_df['temp_edmonton'].shift(13)
prediction_df['temp_edmonton_lag14'] = prediction_df['temp_edmonton'].shift(14)
prediction_df['temp_edmonton_lag15'] = prediction_df['temp_edmonton'].shift(15)
prediction_df['temp_edmonton_lag16'] = prediction_df['temp_edmonton'].shift(16)
prediction_df['temp_edmonton_lag17'] = prediction_df['temp_edmonton'].shift(17)
prediction_df['temp_edmonton_lag18'] = prediction_df['temp_edmonton'].shift(18)
prediction_df['temp_edmonton_lag19'] = prediction_df['temp_edmonton'].shift(19)
prediction_df['temp_edmonton_lag20'] = prediction_df['temp_edmonton'].shift(20)
prediction_df['temp_edmonton_lag21'] = prediction_df['temp_edmonton'].shift(21)
prediction_df['temp_edmonton_lag22'] = prediction_df['temp_edmonton'].shift(22)
prediction_df['temp_edmonton_lag23'] = prediction_df['temp_edmonton'].shift(23)
prediction_df['temp_edmonton_lag24'] = prediction_df['temp_edmonton'].shift(24)

prediction_df['temp_calgary_lag1'] = prediction_df['temp_calgary'].shift(1)
prediction_df['temp_calgary_lag2'] = prediction_df['temp_calgary'].shift(2)
prediction_df['temp_calgary_lag3'] = prediction_df['temp_calgary'].shift(3)
prediction_df['temp_calgary_lag4'] = prediction_df['temp_calgary'].shift(4)
prediction_df['temp_calgary_lag5'] = prediction_df['temp_calgary'].shift(5)
prediction_df['temp_calgary_lag6'] = prediction_df['temp_calgary'].shift(6)
prediction_df['temp_calgary_lag7'] = prediction_df['temp_calgary'].shift(7)
prediction_df['temp_calgary_lag8'] = prediction_df['temp_calgary'].shift(8)
prediction_df['temp_calgary_lag9'] = prediction_df['temp_calgary'].shift(9)
prediction_df['temp_calgary_lag10'] = prediction_df['temp_calgary'].shift(10)
prediction_df['temp_calgary_lag11'] = prediction_df['temp_calgary'].shift(11)
prediction_df['temp_calgary_lag12'] = prediction_df['temp_calgary'].shift(12)
prediction_df['temp_calgary_lag13'] = prediction_df['temp_calgary'].shift(13)
prediction_df['temp_calgary_lag14'] = prediction_df['temp_calgary'].shift(14)
prediction_df['temp_calgary_lag15'] = prediction_df['temp_calgary'].shift(15)
prediction_df['temp_calgary_lag16'] = prediction_df['temp_calgary'].shift(16)
prediction_df['temp_calgary_lag17'] = prediction_df['temp_calgary'].shift(17)
prediction_df['temp_calgary_lag18'] = prediction_df['temp_calgary'].shift(18)
prediction_df['temp_calgary_lag19'] = prediction_df['temp_calgary'].shift(19)
prediction_df['temp_calgary_lag20'] = prediction_df['temp_calgary'].shift(20)
prediction_df['temp_calgary_lag21'] = prediction_df['temp_calgary'].shift(21)
prediction_df['temp_calgary_lag22'] = prediction_df['temp_calgary'].shift(22)
prediction_df['temp_calgary_lag23'] = prediction_df['temp_calgary'].shift(23)
prediction_df['temp_calgary_lag24'] = prediction_df['temp_calgary'].shift(24)

'''
Fetching past day weather forecast data  
'''

datetime_pastday   = now_time + pd.Timedelta(days= -1)
fromDate_past      = '{}/{}/{}'.format(datetime_pastday.month,datetime_pastday.day, datetime_pastday.year )
toDate_past        = fromDate_past

df_clgry   = pd.DataFrame()
df_edmtn   = pd.DataFrame()
df_list = {}

streams = [242498, 242497]
city_names = ['calgary', 'edmonton']
k = 0

for i in streams:
    stream_data = nrgStreamApi.GetStreamDataByStreamId([i], fromDate, toDate, 'csv', '')        
    STREAM_DATA = StringIO(stream_data)
    df = pd.read_csv(STREAM_DATA, sep=";")
    
    df = df[16:(16+24)] # removing header information
    df.columns = ["Datetime,temp,wind,direction"]
    new = df['Datetime,temp,wind,direction'].str.split(",", n = 4, expand = True) 
    # making separate first name column from new data frame 
    df["Datetime"]= new[0] 
    df["temp"]= new[1] 
    df["wind"] = new [2]
    df["direction"] = new[3]
    df = df.drop(['Datetime,temp,wind,direction','direction','Datetime', 'wind'],axis=1)
    df = df.reset_index(drop=True)
    df.columns = [str(col) + '_'+ city_names[k] for col in df.columns]
    df_list[k] = df
    k = k+1

df_clgry   = df_list[0]
df_edmtn   = df_list[1]

alberta_weather_merged = pd.concat([df_clgry, df_edmtn], axis=1)
alberta_weather_merged['temp_calgary'] = alberta_weather_merged['temp_calgary'].astype('float64')
alberta_weather_merged['temp_edmonton'] = alberta_weather_merged['temp_edmonton'].astype('float64')


'''
filling up the lagged weather input cells with past day data
'''


#edmonton start

prediction_df.iloc[0,  prediction_df.columns.get_loc('temp_edmonton_lag1')] = alberta_weather_merged.iloc[23,1]

prediction_df.iloc[0,  prediction_df.columns.get_loc('temp_edmonton_lag2')] = alberta_weather_merged.iloc[22,1]
prediction_df.iloc[1,  prediction_df.columns.get_loc('temp_edmonton_lag2')] = alberta_weather_merged.iloc[23,1]

prediction_df.iloc[0,  prediction_df.columns.get_loc('temp_edmonton_lag3')] = alberta_weather_merged.iloc[21,1]
prediction_df.iloc[1,  prediction_df.columns.get_loc('temp_edmonton_lag3')] = alberta_weather_merged.iloc[22,1]
prediction_df.iloc[2,  prediction_df.columns.get_loc('temp_edmonton_lag3')] = alberta_weather_merged.iloc[23,1]


prediction_df.iloc[0,  prediction_df.columns.get_loc('temp_edmonton_lag4')] = alberta_weather_merged.iloc[20,1]
prediction_df.iloc[1,  prediction_df.columns.get_loc('temp_edmonton_lag4')] = alberta_weather_merged.iloc[21,1]
prediction_df.iloc[2,  prediction_df.columns.get_loc('temp_edmonton_lag4')] = alberta_weather_merged.iloc[22,1]
prediction_df.iloc[3,  prediction_df.columns.get_loc('temp_edmonton_lag4')] = alberta_weather_merged.iloc[23,1]


prediction_df.iloc[0,  prediction_df.columns.get_loc('temp_edmonton_lag5')] = alberta_weather_merged.iloc[19,1]
prediction_df.iloc[1,  prediction_df.columns.get_loc('temp_edmonton_lag5')] = alberta_weather_merged.iloc[20,1]
prediction_df.iloc[2,  prediction_df.columns.get_loc('temp_edmonton_lag5')] = alberta_weather_merged.iloc[21,1]
prediction_df.iloc[3,  prediction_df.columns.get_loc('temp_edmonton_lag5')] = alberta_weather_merged.iloc[22,1]
prediction_df.iloc[4,  prediction_df.columns.get_loc('temp_edmonton_lag5')] = alberta_weather_merged.iloc[23,1]


prediction_df.iloc[0,  prediction_df.columns.get_loc('temp_edmonton_lag6')] = alberta_weather_merged.iloc[18,1]
prediction_df.iloc[1,  prediction_df.columns.get_loc('temp_edmonton_lag6')] = alberta_weather_merged.iloc[19,1]
prediction_df.iloc[2,  prediction_df.columns.get_loc('temp_edmonton_lag6')] = alberta_weather_merged.iloc[20,1]
prediction_df.iloc[3,  prediction_df.columns.get_loc('temp_edmonton_lag6')] = alberta_weather_merged.iloc[21,1]
prediction_df.iloc[4,  prediction_df.columns.get_loc('temp_edmonton_lag6')] = alberta_weather_merged.iloc[22,1]
prediction_df.iloc[5,  prediction_df.columns.get_loc('temp_edmonton_lag6')] = alberta_weather_merged.iloc[23,1]


prediction_df.iloc[0,  prediction_df.columns.get_loc('temp_edmonton_lag7')] = alberta_weather_merged.iloc[17,1]
prediction_df.iloc[1,  prediction_df.columns.get_loc('temp_edmonton_lag7')] = alberta_weather_merged.iloc[18,1]
prediction_df.iloc[2,  prediction_df.columns.get_loc('temp_edmonton_lag7')] = alberta_weather_merged.iloc[19,1]
prediction_df.iloc[3,  prediction_df.columns.get_loc('temp_edmonton_lag7')] = alberta_weather_merged.iloc[20,1]
prediction_df.iloc[4,  prediction_df.columns.get_loc('temp_edmonton_lag7')] = alberta_weather_merged.iloc[21,1]
prediction_df.iloc[5,  prediction_df.columns.get_loc('temp_edmonton_lag7')] = alberta_weather_merged.iloc[22,1]
prediction_df.iloc[6,  prediction_df.columns.get_loc('temp_edmonton_lag7')] = alberta_weather_merged.iloc[23,1]


prediction_df.iloc[0,  prediction_df.columns.get_loc('temp_edmonton_lag8')] = alberta_weather_merged.iloc[16,1]
prediction_df.iloc[1,  prediction_df.columns.get_loc('temp_edmonton_lag8')] = alberta_weather_merged.iloc[17,1]
prediction_df.iloc[2,  prediction_df.columns.get_loc('temp_edmonton_lag8')] = alberta_weather_merged.iloc[18,1]
prediction_df.iloc[3,  prediction_df.columns.get_loc('temp_edmonton_lag8')] = alberta_weather_merged.iloc[19,1]
prediction_df.iloc[4,  prediction_df.columns.get_loc('temp_edmonton_lag8')] = alberta_weather_merged.iloc[20,1]
prediction_df.iloc[5,  prediction_df.columns.get_loc('temp_edmonton_lag8')] = alberta_weather_merged.iloc[21,1]
prediction_df.iloc[6,  prediction_df.columns.get_loc('temp_edmonton_lag8')] = alberta_weather_merged.iloc[22,1]
prediction_df.iloc[7,  prediction_df.columns.get_loc('temp_edmonton_lag8')] = alberta_weather_merged.iloc[23,1]


prediction_df.iloc[0,  prediction_df.columns.get_loc('temp_edmonton_lag9')] = alberta_weather_merged.iloc[15,1]
prediction_df.iloc[1,  prediction_df.columns.get_loc('temp_edmonton_lag9')] = alberta_weather_merged.iloc[16,1]
prediction_df.iloc[2,  prediction_df.columns.get_loc('temp_edmonton_lag9')] = alberta_weather_merged.iloc[17,1]
prediction_df.iloc[3,  prediction_df.columns.get_loc('temp_edmonton_lag9')] = alberta_weather_merged.iloc[18,1]
prediction_df.iloc[4,  prediction_df.columns.get_loc('temp_edmonton_lag9')] = alberta_weather_merged.iloc[19,1]
prediction_df.iloc[5,  prediction_df.columns.get_loc('temp_edmonton_lag9')] = alberta_weather_merged.iloc[20,1]
prediction_df.iloc[6,  prediction_df.columns.get_loc('temp_edmonton_lag9')] = alberta_weather_merged.iloc[21,1]
prediction_df.iloc[7,  prediction_df.columns.get_loc('temp_edmonton_lag9')] = alberta_weather_merged.iloc[22,1]
prediction_df.iloc[8,  prediction_df.columns.get_loc('temp_edmonton_lag9')] = alberta_weather_merged.iloc[23,1]


prediction_df.iloc[0, prediction_df.columns.get_loc('temp_edmonton_lag10')] = alberta_weather_merged.iloc[14,1]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_edmonton_lag10')] = alberta_weather_merged.iloc[15,1]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_edmonton_lag10')] = alberta_weather_merged.iloc[16,1]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_edmonton_lag10')] = alberta_weather_merged.iloc[17,1]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_edmonton_lag10')] = alberta_weather_merged.iloc[18,1]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_edmonton_lag10')] = alberta_weather_merged.iloc[19,1]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_edmonton_lag10')] = alberta_weather_merged.iloc[20,1]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_edmonton_lag10')] = alberta_weather_merged.iloc[21,1]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_edmonton_lag10')] = alberta_weather_merged.iloc[22,1]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_edmonton_lag10')] = alberta_weather_merged.iloc[23,1]




prediction_df.iloc[0, prediction_df.columns.get_loc('temp_edmonton_lag11')] = alberta_weather_merged.iloc[13,1]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_edmonton_lag11')] = alberta_weather_merged.iloc[14,1]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_edmonton_lag11')] = alberta_weather_merged.iloc[15,1]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_edmonton_lag11')] = alberta_weather_merged.iloc[16,1]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_edmonton_lag11')] = alberta_weather_merged.iloc[17,1]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_edmonton_lag11')] = alberta_weather_merged.iloc[18,1]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_edmonton_lag11')] = alberta_weather_merged.iloc[19,1]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_edmonton_lag11')] = alberta_weather_merged.iloc[20,1]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_edmonton_lag11')] = alberta_weather_merged.iloc[21,1]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_edmonton_lag11')] = alberta_weather_merged.iloc[22,1]
prediction_df.iloc[10, prediction_df.columns.get_loc('temp_edmonton_lag11')] = alberta_weather_merged.iloc[23,1]


prediction_df.iloc[0, prediction_df.columns.get_loc('temp_edmonton_lag12')] = alberta_weather_merged.iloc[12,1]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_edmonton_lag12')] = alberta_weather_merged.iloc[13,1]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_edmonton_lag12')] = alberta_weather_merged.iloc[14,1]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_edmonton_lag12')] = alberta_weather_merged.iloc[15,1]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_edmonton_lag12')] = alberta_weather_merged.iloc[16,1]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_edmonton_lag12')] = alberta_weather_merged.iloc[17,1]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_edmonton_lag12')] = alberta_weather_merged.iloc[18,1]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_edmonton_lag12')] = alberta_weather_merged.iloc[19,1]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_edmonton_lag12')] = alberta_weather_merged.iloc[20,1]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_edmonton_lag12')] = alberta_weather_merged.iloc[21,1]
prediction_df.iloc[10, prediction_df.columns.get_loc('temp_edmonton_lag12')] = alberta_weather_merged.iloc[22,1]
prediction_df.iloc[11, prediction_df.columns.get_loc('temp_edmonton_lag12')] = alberta_weather_merged.iloc[23,1]

prediction_df.iloc[0, prediction_df.columns.get_loc('temp_edmonton_lag13')] = alberta_weather_merged.iloc[11,1]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_edmonton_lag13')] = alberta_weather_merged.iloc[12,1]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_edmonton_lag13')] = alberta_weather_merged.iloc[13,1]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_edmonton_lag13')] = alberta_weather_merged.iloc[14,1]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_edmonton_lag13')] = alberta_weather_merged.iloc[15,1]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_edmonton_lag13')] = alberta_weather_merged.iloc[16,1]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_edmonton_lag13')] = alberta_weather_merged.iloc[17,1]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_edmonton_lag13')] = alberta_weather_merged.iloc[18,1]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_edmonton_lag13')] = alberta_weather_merged.iloc[19,1]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_edmonton_lag13')] = alberta_weather_merged.iloc[20,1]
prediction_df.iloc[10, prediction_df.columns.get_loc('temp_edmonton_lag13')] = alberta_weather_merged.iloc[21,1]
prediction_df.iloc[11, prediction_df.columns.get_loc('temp_edmonton_lag13')] = alberta_weather_merged.iloc[22,1]
prediction_df.iloc[12, prediction_df.columns.get_loc('temp_edmonton_lag13')] = alberta_weather_merged.iloc[23,1]

prediction_df.iloc[0, prediction_df.columns.get_loc('temp_edmonton_lag14')] = alberta_weather_merged.iloc[10,1]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_edmonton_lag14')] = alberta_weather_merged.iloc[11,1]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_edmonton_lag14')] = alberta_weather_merged.iloc[12,1]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_edmonton_lag14')] = alberta_weather_merged.iloc[13,1]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_edmonton_lag14')] = alberta_weather_merged.iloc[14,1]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_edmonton_lag14')] = alberta_weather_merged.iloc[15,1]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_edmonton_lag14')] = alberta_weather_merged.iloc[16,1]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_edmonton_lag14')] = alberta_weather_merged.iloc[17,1]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_edmonton_lag14')] = alberta_weather_merged.iloc[18,1]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_edmonton_lag14')] = alberta_weather_merged.iloc[19,1]
prediction_df.iloc[10, prediction_df.columns.get_loc('temp_edmonton_lag14')] = alberta_weather_merged.iloc[20,1]
prediction_df.iloc[11, prediction_df.columns.get_loc('temp_edmonton_lag14')] = alberta_weather_merged.iloc[21,1]
prediction_df.iloc[12, prediction_df.columns.get_loc('temp_edmonton_lag14')] = alberta_weather_merged.iloc[22,1]
prediction_df.iloc[13, prediction_df.columns.get_loc('temp_edmonton_lag14')] = alberta_weather_merged.iloc[23,1]


prediction_df.iloc[0, prediction_df.columns.get_loc('temp_edmonton_lag15')] = alberta_weather_merged.iloc[9,1]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_edmonton_lag15')] = alberta_weather_merged.iloc[10,1]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_edmonton_lag15')] = alberta_weather_merged.iloc[11,1]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_edmonton_lag15')] = alberta_weather_merged.iloc[12,1]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_edmonton_lag15')] = alberta_weather_merged.iloc[13,1]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_edmonton_lag15')] = alberta_weather_merged.iloc[14,1]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_edmonton_lag15')] = alberta_weather_merged.iloc[15,1]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_edmonton_lag15')] = alberta_weather_merged.iloc[16,1]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_edmonton_lag15')] = alberta_weather_merged.iloc[17,1]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_edmonton_lag15')] = alberta_weather_merged.iloc[18,1]
prediction_df.iloc[10, prediction_df.columns.get_loc('temp_edmonton_lag15')] = alberta_weather_merged.iloc[19,1]
prediction_df.iloc[11, prediction_df.columns.get_loc('temp_edmonton_lag15')] = alberta_weather_merged.iloc[20,1]
prediction_df.iloc[12, prediction_df.columns.get_loc('temp_edmonton_lag15')] = alberta_weather_merged.iloc[21,1]
prediction_df.iloc[13, prediction_df.columns.get_loc('temp_edmonton_lag15')] = alberta_weather_merged.iloc[22,1]
prediction_df.iloc[14, prediction_df.columns.get_loc('temp_edmonton_lag15')] = alberta_weather_merged.iloc[23,1]


prediction_df.iloc[0, prediction_df.columns.get_loc('temp_edmonton_lag16')] = alberta_weather_merged.iloc[8,1]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_edmonton_lag16')] = alberta_weather_merged.iloc[9,1]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_edmonton_lag16')] = alberta_weather_merged.iloc[10,1]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_edmonton_lag16')] = alberta_weather_merged.iloc[11,1]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_edmonton_lag16')] = alberta_weather_merged.iloc[12,1]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_edmonton_lag16')] = alberta_weather_merged.iloc[13,1]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_edmonton_lag16')] = alberta_weather_merged.iloc[14,1]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_edmonton_lag16')] = alberta_weather_merged.iloc[15,1]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_edmonton_lag16')] = alberta_weather_merged.iloc[16,1]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_edmonton_lag16')] = alberta_weather_merged.iloc[17,1]
prediction_df.iloc[10, prediction_df.columns.get_loc('temp_edmonton_lag16')] = alberta_weather_merged.iloc[18,1]
prediction_df.iloc[11, prediction_df.columns.get_loc('temp_edmonton_lag16')] = alberta_weather_merged.iloc[19,1]
prediction_df.iloc[12, prediction_df.columns.get_loc('temp_edmonton_lag16')] = alberta_weather_merged.iloc[20,1]
prediction_df.iloc[13, prediction_df.columns.get_loc('temp_edmonton_lag16')] = alberta_weather_merged.iloc[21,1]
prediction_df.iloc[14, prediction_df.columns.get_loc('temp_edmonton_lag16')] = alberta_weather_merged.iloc[22,1]
prediction_df.iloc[15, prediction_df.columns.get_loc('temp_edmonton_lag16')] = alberta_weather_merged.iloc[23,1]



prediction_df.iloc[0, prediction_df.columns.get_loc('temp_edmonton_lag17')] = alberta_weather_merged.iloc[7,1]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_edmonton_lag17')] = alberta_weather_merged.iloc[8,1]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_edmonton_lag17')] = alberta_weather_merged.iloc[9,1]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_edmonton_lag17')] = alberta_weather_merged.iloc[10,1]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_edmonton_lag17')] = alberta_weather_merged.iloc[11,1]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_edmonton_lag17')] = alberta_weather_merged.iloc[12,1]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_edmonton_lag17')] = alberta_weather_merged.iloc[13,1]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_edmonton_lag17')] = alberta_weather_merged.iloc[14,1]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_edmonton_lag17')] = alberta_weather_merged.iloc[15,1]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_edmonton_lag17')] = alberta_weather_merged.iloc[16,1]
prediction_df.iloc[10, prediction_df.columns.get_loc('temp_edmonton_lag17')] = alberta_weather_merged.iloc[17,1]
prediction_df.iloc[11, prediction_df.columns.get_loc('temp_edmonton_lag17')] = alberta_weather_merged.iloc[18,1]
prediction_df.iloc[12, prediction_df.columns.get_loc('temp_edmonton_lag17')] = alberta_weather_merged.iloc[19,1]
prediction_df.iloc[13, prediction_df.columns.get_loc('temp_edmonton_lag17')] = alberta_weather_merged.iloc[20,1]
prediction_df.iloc[14, prediction_df.columns.get_loc('temp_edmonton_lag17')] = alberta_weather_merged.iloc[21,1]
prediction_df.iloc[15, prediction_df.columns.get_loc('temp_edmonton_lag17')] = alberta_weather_merged.iloc[22,1]
prediction_df.iloc[16, prediction_df.columns.get_loc('temp_edmonton_lag17')] = alberta_weather_merged.iloc[23,1]



prediction_df.iloc[0, prediction_df.columns.get_loc('temp_edmonton_lag18')] = alberta_weather_merged.iloc[6,1]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_edmonton_lag18')] = alberta_weather_merged.iloc[7,1]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_edmonton_lag18')] = alberta_weather_merged.iloc[8,1]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_edmonton_lag18')] = alberta_weather_merged.iloc[9,1]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_edmonton_lag18')] = alberta_weather_merged.iloc[10,1]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_edmonton_lag18')] = alberta_weather_merged.iloc[11,1]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_edmonton_lag18')] = alberta_weather_merged.iloc[12,1]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_edmonton_lag18')] = alberta_weather_merged.iloc[13,1]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_edmonton_lag18')] = alberta_weather_merged.iloc[14,1]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_edmonton_lag18')] = alberta_weather_merged.iloc[15,1]
prediction_df.iloc[10, prediction_df.columns.get_loc('temp_edmonton_lag18')] = alberta_weather_merged.iloc[16,1]
prediction_df.iloc[11, prediction_df.columns.get_loc('temp_edmonton_lag18')] = alberta_weather_merged.iloc[17,1]
prediction_df.iloc[12, prediction_df.columns.get_loc('temp_edmonton_lag18')] = alberta_weather_merged.iloc[18,1]
prediction_df.iloc[13, prediction_df.columns.get_loc('temp_edmonton_lag18')] = alberta_weather_merged.iloc[19,1]
prediction_df.iloc[14, prediction_df.columns.get_loc('temp_edmonton_lag18')] = alberta_weather_merged.iloc[20,1]
prediction_df.iloc[15, prediction_df.columns.get_loc('temp_edmonton_lag18')] = alberta_weather_merged.iloc[21,1]
prediction_df.iloc[16, prediction_df.columns.get_loc('temp_edmonton_lag18')] = alberta_weather_merged.iloc[22,1]
prediction_df.iloc[17, prediction_df.columns.get_loc('temp_edmonton_lag18')] = alberta_weather_merged.iloc[23,1]




prediction_df.iloc[0, prediction_df.columns.get_loc('temp_edmonton_lag19')] = alberta_weather_merged.iloc[5,1]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_edmonton_lag19')] = alberta_weather_merged.iloc[6,1]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_edmonton_lag19')] = alberta_weather_merged.iloc[7,1]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_edmonton_lag19')] = alberta_weather_merged.iloc[8,1]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_edmonton_lag19')] = alberta_weather_merged.iloc[9,1]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_edmonton_lag19')] = alberta_weather_merged.iloc[10,1]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_edmonton_lag19')] = alberta_weather_merged.iloc[11,1]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_edmonton_lag19')] = alberta_weather_merged.iloc[12,1]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_edmonton_lag19')] = alberta_weather_merged.iloc[13,1]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_edmonton_lag19')] = alberta_weather_merged.iloc[14,1]
prediction_df.iloc[10, prediction_df.columns.get_loc('temp_edmonton_lag19')] = alberta_weather_merged.iloc[15,1]
prediction_df.iloc[11, prediction_df.columns.get_loc('temp_edmonton_lag19')] = alberta_weather_merged.iloc[16,1]
prediction_df.iloc[12, prediction_df.columns.get_loc('temp_edmonton_lag19')] = alberta_weather_merged.iloc[17,1]
prediction_df.iloc[13, prediction_df.columns.get_loc('temp_edmonton_lag19')] = alberta_weather_merged.iloc[18,1]
prediction_df.iloc[14, prediction_df.columns.get_loc('temp_edmonton_lag19')] = alberta_weather_merged.iloc[19,1]
prediction_df.iloc[15, prediction_df.columns.get_loc('temp_edmonton_lag19')] = alberta_weather_merged.iloc[20,1]
prediction_df.iloc[16, prediction_df.columns.get_loc('temp_edmonton_lag19')] = alberta_weather_merged.iloc[21,1]
prediction_df.iloc[17, prediction_df.columns.get_loc('temp_edmonton_lag19')] = alberta_weather_merged.iloc[22,1]
prediction_df.iloc[18, prediction_df.columns.get_loc('temp_edmonton_lag19')] = alberta_weather_merged.iloc[23,1]



prediction_df.iloc[0, prediction_df.columns.get_loc('temp_edmonton_lag20')] = alberta_weather_merged.iloc[4,1]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_edmonton_lag20')] = alberta_weather_merged.iloc[5,1]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_edmonton_lag20')] = alberta_weather_merged.iloc[6,1]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_edmonton_lag20')] = alberta_weather_merged.iloc[7,1]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_edmonton_lag20')] = alberta_weather_merged.iloc[8,1]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_edmonton_lag20')] = alberta_weather_merged.iloc[9,1]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_edmonton_lag20')] = alberta_weather_merged.iloc[10,1]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_edmonton_lag20')] = alberta_weather_merged.iloc[11,1]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_edmonton_lag20')] = alberta_weather_merged.iloc[12,1]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_edmonton_lag20')] = alberta_weather_merged.iloc[13,1]
prediction_df.iloc[10, prediction_df.columns.get_loc('temp_edmonton_lag20')] = alberta_weather_merged.iloc[14,1]
prediction_df.iloc[11, prediction_df.columns.get_loc('temp_edmonton_lag20')] = alberta_weather_merged.iloc[15,1]
prediction_df.iloc[12, prediction_df.columns.get_loc('temp_edmonton_lag20')] = alberta_weather_merged.iloc[16,1]
prediction_df.iloc[13, prediction_df.columns.get_loc('temp_edmonton_lag20')] = alberta_weather_merged.iloc[17,1]
prediction_df.iloc[14, prediction_df.columns.get_loc('temp_edmonton_lag20')] = alberta_weather_merged.iloc[18,1]
prediction_df.iloc[15, prediction_df.columns.get_loc('temp_edmonton_lag20')] = alberta_weather_merged.iloc[19,1]
prediction_df.iloc[16, prediction_df.columns.get_loc('temp_edmonton_lag20')] = alberta_weather_merged.iloc[20,1]
prediction_df.iloc[17, prediction_df.columns.get_loc('temp_edmonton_lag20')] = alberta_weather_merged.iloc[21,1]
prediction_df.iloc[18, prediction_df.columns.get_loc('temp_edmonton_lag20')] = alberta_weather_merged.iloc[22,1]
prediction_df.iloc[19, prediction_df.columns.get_loc('temp_edmonton_lag20')] = alberta_weather_merged.iloc[23,1]




prediction_df.iloc[0, prediction_df.columns.get_loc('temp_edmonton_lag21')] = alberta_weather_merged.iloc[3,1]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_edmonton_lag21')] = alberta_weather_merged.iloc[4,1]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_edmonton_lag21')] = alberta_weather_merged.iloc[5,1]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_edmonton_lag21')] = alberta_weather_merged.iloc[6,1]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_edmonton_lag21')] = alberta_weather_merged.iloc[7,1]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_edmonton_lag21')] = alberta_weather_merged.iloc[8,1]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_edmonton_lag21')] = alberta_weather_merged.iloc[9,1]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_edmonton_lag21')] = alberta_weather_merged.iloc[10,1]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_edmonton_lag21')] = alberta_weather_merged.iloc[11,1]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_edmonton_lag21')] = alberta_weather_merged.iloc[12,1]
prediction_df.iloc[10, prediction_df.columns.get_loc('temp_edmonton_lag21')] = alberta_weather_merged.iloc[13,1]
prediction_df.iloc[11, prediction_df.columns.get_loc('temp_edmonton_lag21')] = alberta_weather_merged.iloc[14,1]
prediction_df.iloc[12, prediction_df.columns.get_loc('temp_edmonton_lag21')] = alberta_weather_merged.iloc[15,1]
prediction_df.iloc[13, prediction_df.columns.get_loc('temp_edmonton_lag21')] = alberta_weather_merged.iloc[16,1]
prediction_df.iloc[14, prediction_df.columns.get_loc('temp_edmonton_lag21')] = alberta_weather_merged.iloc[17,1]
prediction_df.iloc[15, prediction_df.columns.get_loc('temp_edmonton_lag21')] = alberta_weather_merged.iloc[18,1]
prediction_df.iloc[16, prediction_df.columns.get_loc('temp_edmonton_lag21')] = alberta_weather_merged.iloc[19,1]
prediction_df.iloc[17, prediction_df.columns.get_loc('temp_edmonton_lag21')] = alberta_weather_merged.iloc[20,1]
prediction_df.iloc[18, prediction_df.columns.get_loc('temp_edmonton_lag21')] = alberta_weather_merged.iloc[21,1]
prediction_df.iloc[19, prediction_df.columns.get_loc('temp_edmonton_lag21')] = alberta_weather_merged.iloc[22,1]
prediction_df.iloc[20, prediction_df.columns.get_loc('temp_edmonton_lag21')] = alberta_weather_merged.iloc[23,1]



prediction_df.iloc[0, prediction_df.columns.get_loc('temp_edmonton_lag22')] = alberta_weather_merged.iloc[2,1]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_edmonton_lag22')] = alberta_weather_merged.iloc[3,1]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_edmonton_lag22')] = alberta_weather_merged.iloc[4,1]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_edmonton_lag22')] = alberta_weather_merged.iloc[5,1]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_edmonton_lag22')] = alberta_weather_merged.iloc[6,1]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_edmonton_lag22')] = alberta_weather_merged.iloc[7,1]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_edmonton_lag22')] = alberta_weather_merged.iloc[8,1]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_edmonton_lag22')] = alberta_weather_merged.iloc[9,1]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_edmonton_lag22')] = alberta_weather_merged.iloc[10,1]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_edmonton_lag22')] = alberta_weather_merged.iloc[11,1]
prediction_df.iloc[10, prediction_df.columns.get_loc('temp_edmonton_lag22')] = alberta_weather_merged.iloc[12,1]
prediction_df.iloc[11, prediction_df.columns.get_loc('temp_edmonton_lag22')] = alberta_weather_merged.iloc[13,1]
prediction_df.iloc[12, prediction_df.columns.get_loc('temp_edmonton_lag22')] = alberta_weather_merged.iloc[14,1]
prediction_df.iloc[13, prediction_df.columns.get_loc('temp_edmonton_lag22')] = alberta_weather_merged.iloc[15,1]
prediction_df.iloc[14, prediction_df.columns.get_loc('temp_edmonton_lag22')] = alberta_weather_merged.iloc[16,1]
prediction_df.iloc[15, prediction_df.columns.get_loc('temp_edmonton_lag22')] = alberta_weather_merged.iloc[17,1]
prediction_df.iloc[16, prediction_df.columns.get_loc('temp_edmonton_lag22')] = alberta_weather_merged.iloc[18,1]
prediction_df.iloc[17, prediction_df.columns.get_loc('temp_edmonton_lag22')] = alberta_weather_merged.iloc[19,1]
prediction_df.iloc[18, prediction_df.columns.get_loc('temp_edmonton_lag22')] = alberta_weather_merged.iloc[20,1]
prediction_df.iloc[19, prediction_df.columns.get_loc('temp_edmonton_lag22')] = alberta_weather_merged.iloc[21,1]
prediction_df.iloc[20, prediction_df.columns.get_loc('temp_edmonton_lag22')] = alberta_weather_merged.iloc[22,1]
prediction_df.iloc[21, prediction_df.columns.get_loc('temp_edmonton_lag22')] = alberta_weather_merged.iloc[23,1]



prediction_df.iloc[0, prediction_df.columns.get_loc('temp_edmonton_lag23')] = alberta_weather_merged.iloc[1,1]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_edmonton_lag23')] = alberta_weather_merged.iloc[2,1]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_edmonton_lag23')] = alberta_weather_merged.iloc[3,1]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_edmonton_lag23')] = alberta_weather_merged.iloc[4,1]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_edmonton_lag23')] = alberta_weather_merged.iloc[5,1]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_edmonton_lag23')] = alberta_weather_merged.iloc[6,1]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_edmonton_lag23')] = alberta_weather_merged.iloc[7,1]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_edmonton_lag23')] = alberta_weather_merged.iloc[8,1]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_edmonton_lag23')] = alberta_weather_merged.iloc[9,1]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_edmonton_lag23')] = alberta_weather_merged.iloc[10,1]
prediction_df.iloc[10, prediction_df.columns.get_loc('temp_edmonton_lag23')] = alberta_weather_merged.iloc[11,1]
prediction_df.iloc[11, prediction_df.columns.get_loc('temp_edmonton_lag23')] = alberta_weather_merged.iloc[12,1]
prediction_df.iloc[12, prediction_df.columns.get_loc('temp_edmonton_lag23')] = alberta_weather_merged.iloc[13,1]
prediction_df.iloc[13, prediction_df.columns.get_loc('temp_edmonton_lag23')] = alberta_weather_merged.iloc[14,1]
prediction_df.iloc[14, prediction_df.columns.get_loc('temp_edmonton_lag23')] = alberta_weather_merged.iloc[15,1]
prediction_df.iloc[15, prediction_df.columns.get_loc('temp_edmonton_lag23')] = alberta_weather_merged.iloc[16,1]
prediction_df.iloc[16, prediction_df.columns.get_loc('temp_edmonton_lag23')] = alberta_weather_merged.iloc[17,1]
prediction_df.iloc[17, prediction_df.columns.get_loc('temp_edmonton_lag23')] = alberta_weather_merged.iloc[18,1]
prediction_df.iloc[18, prediction_df.columns.get_loc('temp_edmonton_lag23')] = alberta_weather_merged.iloc[19,1]
prediction_df.iloc[19, prediction_df.columns.get_loc('temp_edmonton_lag23')] = alberta_weather_merged.iloc[20,1]
prediction_df.iloc[20, prediction_df.columns.get_loc('temp_edmonton_lag23')] = alberta_weather_merged.iloc[21,1]
prediction_df.iloc[21, prediction_df.columns.get_loc('temp_edmonton_lag23')] = alberta_weather_merged.iloc[22,1]
prediction_df.iloc[22, prediction_df.columns.get_loc('temp_edmonton_lag23')] = alberta_weather_merged.iloc[23,1]



prediction_df.iloc[0, prediction_df.columns.get_loc('temp_edmonton_lag24')] = alberta_weather_merged.iloc[0,1]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_edmonton_lag24')] = alberta_weather_merged.iloc[1,1]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_edmonton_lag24')] = alberta_weather_merged.iloc[2,1]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_edmonton_lag24')] = alberta_weather_merged.iloc[3,1]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_edmonton_lag24')] = alberta_weather_merged.iloc[4,1]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_edmonton_lag24')] = alberta_weather_merged.iloc[5,1]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_edmonton_lag24')] = alberta_weather_merged.iloc[6,1]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_edmonton_lag24')] = alberta_weather_merged.iloc[7,1]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_edmonton_lag24')] = alberta_weather_merged.iloc[8,1]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_edmonton_lag24')] = alberta_weather_merged.iloc[9,1]
prediction_df.iloc[10, prediction_df.columns.get_loc('temp_edmonton_lag24')] = alberta_weather_merged.iloc[10,1]
prediction_df.iloc[11, prediction_df.columns.get_loc('temp_edmonton_lag24')] = alberta_weather_merged.iloc[11,1]
prediction_df.iloc[12, prediction_df.columns.get_loc('temp_edmonton_lag24')] = alberta_weather_merged.iloc[12,1]
prediction_df.iloc[13, prediction_df.columns.get_loc('temp_edmonton_lag24')] = alberta_weather_merged.iloc[13,1]
prediction_df.iloc[14, prediction_df.columns.get_loc('temp_edmonton_lag24')] = alberta_weather_merged.iloc[14,1]
prediction_df.iloc[15, prediction_df.columns.get_loc('temp_edmonton_lag24')] = alberta_weather_merged.iloc[15,1]
prediction_df.iloc[16, prediction_df.columns.get_loc('temp_edmonton_lag24')] = alberta_weather_merged.iloc[16,1]
prediction_df.iloc[17, prediction_df.columns.get_loc('temp_edmonton_lag24')] = alberta_weather_merged.iloc[17,1]
prediction_df.iloc[18, prediction_df.columns.get_loc('temp_edmonton_lag24')] = alberta_weather_merged.iloc[18,1]
prediction_df.iloc[19, prediction_df.columns.get_loc('temp_edmonton_lag24')] = alberta_weather_merged.iloc[19,1]
prediction_df.iloc[20, prediction_df.columns.get_loc('temp_edmonton_lag24')] = alberta_weather_merged.iloc[20,1]
prediction_df.iloc[21, prediction_df.columns.get_loc('temp_edmonton_lag24')] = alberta_weather_merged.iloc[21,1]
prediction_df.iloc[22, prediction_df.columns.get_loc('temp_edmonton_lag24')] = alberta_weather_merged.iloc[22,1]
prediction_df.iloc[23, prediction_df.columns.get_loc('temp_edmonton_lag24')] = alberta_weather_merged.iloc[23,1]


# calgary start


prediction_df.iloc[0,  prediction_df.columns.get_loc('temp_calgary_lag1')] = alberta_weather_merged.iloc[23,0]

prediction_df.iloc[0,  prediction_df.columns.get_loc('temp_calgary_lag2')] = alberta_weather_merged.iloc[22,0]
prediction_df.iloc[1,  prediction_df.columns.get_loc('temp_calgary_lag2')] = alberta_weather_merged.iloc[23,0]

prediction_df.iloc[0,  prediction_df.columns.get_loc('temp_calgary_lag3')] = alberta_weather_merged.iloc[21,0]
prediction_df.iloc[1,  prediction_df.columns.get_loc('temp_calgary_lag3')] = alberta_weather_merged.iloc[22,0]
prediction_df.iloc[2,  prediction_df.columns.get_loc('temp_calgary_lag3')] = alberta_weather_merged.iloc[23,0]


prediction_df.iloc[0,  prediction_df.columns.get_loc('temp_calgary_lag4')] = alberta_weather_merged.iloc[20,0]
prediction_df.iloc[1,  prediction_df.columns.get_loc('temp_calgary_lag4')] = alberta_weather_merged.iloc[21,0]
prediction_df.iloc[2,  prediction_df.columns.get_loc('temp_calgary_lag4')] = alberta_weather_merged.iloc[22,0]
prediction_df.iloc[3,  prediction_df.columns.get_loc('temp_calgary_lag4')] = alberta_weather_merged.iloc[23,0]


prediction_df.iloc[0,  prediction_df.columns.get_loc('temp_calgary_lag5')] = alberta_weather_merged.iloc[19,0]
prediction_df.iloc[1,  prediction_df.columns.get_loc('temp_calgary_lag5')] = alberta_weather_merged.iloc[20,0]
prediction_df.iloc[2,  prediction_df.columns.get_loc('temp_calgary_lag5')] = alberta_weather_merged.iloc[21,0]
prediction_df.iloc[3,  prediction_df.columns.get_loc('temp_calgary_lag5')] = alberta_weather_merged.iloc[22,0]
prediction_df.iloc[4,  prediction_df.columns.get_loc('temp_calgary_lag5')] = alberta_weather_merged.iloc[23,0]


prediction_df.iloc[0,  prediction_df.columns.get_loc('temp_calgary_lag6')] = alberta_weather_merged.iloc[18,0]
prediction_df.iloc[1,  prediction_df.columns.get_loc('temp_calgary_lag6')] = alberta_weather_merged.iloc[19,0]
prediction_df.iloc[2,  prediction_df.columns.get_loc('temp_calgary_lag6')] = alberta_weather_merged.iloc[20,0]
prediction_df.iloc[3,  prediction_df.columns.get_loc('temp_calgary_lag6')] = alberta_weather_merged.iloc[21,0]
prediction_df.iloc[4,  prediction_df.columns.get_loc('temp_calgary_lag6')] = alberta_weather_merged.iloc[22,0]
prediction_df.iloc[5,  prediction_df.columns.get_loc('temp_calgary_lag6')] = alberta_weather_merged.iloc[23,0]


prediction_df.iloc[0,  prediction_df.columns.get_loc('temp_calgary_lag7')] = alberta_weather_merged.iloc[17,0]
prediction_df.iloc[1,  prediction_df.columns.get_loc('temp_calgary_lag7')] = alberta_weather_merged.iloc[18,0]
prediction_df.iloc[2,  prediction_df.columns.get_loc('temp_calgary_lag7')] = alberta_weather_merged.iloc[19,0]
prediction_df.iloc[3,  prediction_df.columns.get_loc('temp_calgary_lag7')] = alberta_weather_merged.iloc[20,0]
prediction_df.iloc[4,  prediction_df.columns.get_loc('temp_calgary_lag7')] = alberta_weather_merged.iloc[21,0]
prediction_df.iloc[5,  prediction_df.columns.get_loc('temp_calgary_lag7')] = alberta_weather_merged.iloc[22,0]
prediction_df.iloc[6,  prediction_df.columns.get_loc('temp_calgary_lag7')] = alberta_weather_merged.iloc[23,0]


prediction_df.iloc[0,  prediction_df.columns.get_loc('temp_calgary_lag8')] = alberta_weather_merged.iloc[16,0]
prediction_df.iloc[1,  prediction_df.columns.get_loc('temp_calgary_lag8')] = alberta_weather_merged.iloc[17,0]
prediction_df.iloc[2,  prediction_df.columns.get_loc('temp_calgary_lag8')] = alberta_weather_merged.iloc[18,0]
prediction_df.iloc[3,  prediction_df.columns.get_loc('temp_calgary_lag8')] = alberta_weather_merged.iloc[19,0]
prediction_df.iloc[4,  prediction_df.columns.get_loc('temp_calgary_lag8')] = alberta_weather_merged.iloc[20,0]
prediction_df.iloc[5,  prediction_df.columns.get_loc('temp_calgary_lag8')] = alberta_weather_merged.iloc[21,0]
prediction_df.iloc[6,  prediction_df.columns.get_loc('temp_calgary_lag8')] = alberta_weather_merged.iloc[22,0]
prediction_df.iloc[7,  prediction_df.columns.get_loc('temp_calgary_lag8')] = alberta_weather_merged.iloc[23,0]


prediction_df.iloc[0,  prediction_df.columns.get_loc('temp_calgary_lag9')] = alberta_weather_merged.iloc[15,0]
prediction_df.iloc[1,  prediction_df.columns.get_loc('temp_calgary_lag9')] = alberta_weather_merged.iloc[16,0]
prediction_df.iloc[2,  prediction_df.columns.get_loc('temp_calgary_lag9')] = alberta_weather_merged.iloc[17,0]
prediction_df.iloc[3,  prediction_df.columns.get_loc('temp_calgary_lag9')] = alberta_weather_merged.iloc[18,0]
prediction_df.iloc[4,  prediction_df.columns.get_loc('temp_calgary_lag9')] = alberta_weather_merged.iloc[19,0]
prediction_df.iloc[5,  prediction_df.columns.get_loc('temp_calgary_lag9')] = alberta_weather_merged.iloc[20,0]
prediction_df.iloc[6,  prediction_df.columns.get_loc('temp_calgary_lag9')] = alberta_weather_merged.iloc[21,0]
prediction_df.iloc[7,  prediction_df.columns.get_loc('temp_calgary_lag9')] = alberta_weather_merged.iloc[22,0]
prediction_df.iloc[8,  prediction_df.columns.get_loc('temp_calgary_lag9')] = alberta_weather_merged.iloc[23,0]


prediction_df.iloc[0, prediction_df.columns.get_loc('temp_calgary_lag10')] = alberta_weather_merged.iloc[14,0]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_calgary_lag10')] = alberta_weather_merged.iloc[15,0]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_calgary_lag10')] = alberta_weather_merged.iloc[16,0]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_calgary_lag10')] = alberta_weather_merged.iloc[17,0]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_calgary_lag10')] = alberta_weather_merged.iloc[18,0]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_calgary_lag10')] = alberta_weather_merged.iloc[19,0]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_calgary_lag10')] = alberta_weather_merged.iloc[20,0]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_calgary_lag10')] = alberta_weather_merged.iloc[21,0]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_calgary_lag10')] = alberta_weather_merged.iloc[22,0]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_calgary_lag10')] = alberta_weather_merged.iloc[23,0]




prediction_df.iloc[0, prediction_df.columns.get_loc('temp_calgary_lag11')] = alberta_weather_merged.iloc[13,0]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_calgary_lag11')] = alberta_weather_merged.iloc[14,0]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_calgary_lag11')] = alberta_weather_merged.iloc[15,0]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_calgary_lag11')] = alberta_weather_merged.iloc[16,0]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_calgary_lag11')] = alberta_weather_merged.iloc[17,0]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_calgary_lag11')] = alberta_weather_merged.iloc[18,0]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_calgary_lag11')] = alberta_weather_merged.iloc[19,0]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_calgary_lag11')] = alberta_weather_merged.iloc[20,0]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_calgary_lag11')] = alberta_weather_merged.iloc[21,0]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_calgary_lag11')] = alberta_weather_merged.iloc[22,0]
prediction_df.iloc[10, prediction_df.columns.get_loc('temp_calgary_lag11')] = alberta_weather_merged.iloc[23,0]


prediction_df.iloc[0, prediction_df.columns.get_loc('temp_calgary_lag12')] = alberta_weather_merged.iloc[12,0]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_calgary_lag12')] = alberta_weather_merged.iloc[13,0]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_calgary_lag12')] = alberta_weather_merged.iloc[14,0]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_calgary_lag12')] = alberta_weather_merged.iloc[15,0]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_calgary_lag12')] = alberta_weather_merged.iloc[16,0]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_calgary_lag12')] = alberta_weather_merged.iloc[17,0]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_calgary_lag12')] = alberta_weather_merged.iloc[18,0]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_calgary_lag12')] = alberta_weather_merged.iloc[19,0]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_calgary_lag12')] = alberta_weather_merged.iloc[20,0]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_calgary_lag12')] = alberta_weather_merged.iloc[21,0]
prediction_df.iloc[10, prediction_df.columns.get_loc('temp_calgary_lag12')] = alberta_weather_merged.iloc[22,0]
prediction_df.iloc[11, prediction_df.columns.get_loc('temp_calgary_lag12')] = alberta_weather_merged.iloc[23,0]

prediction_df.iloc[0, prediction_df.columns.get_loc('temp_calgary_lag13')] = alberta_weather_merged.iloc[11,0]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_calgary_lag13')] = alberta_weather_merged.iloc[12,0]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_calgary_lag13')] = alberta_weather_merged.iloc[13,0]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_calgary_lag13')] = alberta_weather_merged.iloc[14,0]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_calgary_lag13')] = alberta_weather_merged.iloc[15,0]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_calgary_lag13')] = alberta_weather_merged.iloc[16,0]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_calgary_lag13')] = alberta_weather_merged.iloc[17,0]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_calgary_lag13')] = alberta_weather_merged.iloc[18,0]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_calgary_lag13')] = alberta_weather_merged.iloc[19,0]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_calgary_lag13')] = alberta_weather_merged.iloc[20,0]
prediction_df.iloc[10, prediction_df.columns.get_loc('temp_calgary_lag13')] = alberta_weather_merged.iloc[21,0]
prediction_df.iloc[11, prediction_df.columns.get_loc('temp_calgary_lag13')] = alberta_weather_merged.iloc[22,0]
prediction_df.iloc[12, prediction_df.columns.get_loc('temp_calgary_lag13')] = alberta_weather_merged.iloc[23,0]

prediction_df.iloc[0, prediction_df.columns.get_loc('temp_calgary_lag14')] = alberta_weather_merged.iloc[10,0]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_calgary_lag14')] = alberta_weather_merged.iloc[11,0]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_calgary_lag14')] = alberta_weather_merged.iloc[12,0]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_calgary_lag14')] = alberta_weather_merged.iloc[13,0]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_calgary_lag14')] = alberta_weather_merged.iloc[14,0]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_calgary_lag14')] = alberta_weather_merged.iloc[15,0]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_calgary_lag14')] = alberta_weather_merged.iloc[16,0]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_calgary_lag14')] = alberta_weather_merged.iloc[17,0]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_calgary_lag14')] = alberta_weather_merged.iloc[18,0]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_calgary_lag14')] = alberta_weather_merged.iloc[19,0]
prediction_df.iloc[10, prediction_df.columns.get_loc('temp_calgary_lag14')] = alberta_weather_merged.iloc[20,0]
prediction_df.iloc[11, prediction_df.columns.get_loc('temp_calgary_lag14')] = alberta_weather_merged.iloc[21,0]
prediction_df.iloc[12, prediction_df.columns.get_loc('temp_calgary_lag14')] = alberta_weather_merged.iloc[22,0]
prediction_df.iloc[13, prediction_df.columns.get_loc('temp_calgary_lag14')] = alberta_weather_merged.iloc[23,0]


prediction_df.iloc[0, prediction_df.columns.get_loc('temp_calgary_lag15')] = alberta_weather_merged.iloc[9,0]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_calgary_lag15')] = alberta_weather_merged.iloc[10,0]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_calgary_lag15')] = alberta_weather_merged.iloc[11,0]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_calgary_lag15')] = alberta_weather_merged.iloc[12,0]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_calgary_lag15')] = alberta_weather_merged.iloc[13,0]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_calgary_lag15')] = alberta_weather_merged.iloc[14,0]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_calgary_lag15')] = alberta_weather_merged.iloc[15,0]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_calgary_lag15')] = alberta_weather_merged.iloc[16,0]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_calgary_lag15')] = alberta_weather_merged.iloc[17,0]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_calgary_lag15')] = alberta_weather_merged.iloc[18,0]
prediction_df.iloc[10, prediction_df.columns.get_loc('temp_calgary_lag15')] = alberta_weather_merged.iloc[19,0]
prediction_df.iloc[11, prediction_df.columns.get_loc('temp_calgary_lag15')] = alberta_weather_merged.iloc[20,0]
prediction_df.iloc[12, prediction_df.columns.get_loc('temp_calgary_lag15')] = alberta_weather_merged.iloc[21,0]
prediction_df.iloc[13, prediction_df.columns.get_loc('temp_calgary_lag15')] = alberta_weather_merged.iloc[22,0]
prediction_df.iloc[14, prediction_df.columns.get_loc('temp_calgary_lag15')] = alberta_weather_merged.iloc[23,0]


prediction_df.iloc[0, prediction_df.columns.get_loc('temp_calgary_lag16')] = alberta_weather_merged.iloc[8,0]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_calgary_lag16')] = alberta_weather_merged.iloc[9,0]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_calgary_lag16')] = alberta_weather_merged.iloc[10,0]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_calgary_lag16')] = alberta_weather_merged.iloc[11,0]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_calgary_lag16')] = alberta_weather_merged.iloc[12,0]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_calgary_lag16')] = alberta_weather_merged.iloc[13,0]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_calgary_lag16')] = alberta_weather_merged.iloc[14,0]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_calgary_lag16')] = alberta_weather_merged.iloc[15,0]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_calgary_lag16')] = alberta_weather_merged.iloc[16,0]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_calgary_lag16')] = alberta_weather_merged.iloc[17,0]
prediction_df.iloc[10, prediction_df.columns.get_loc('temp_calgary_lag16')] = alberta_weather_merged.iloc[18,0]
prediction_df.iloc[11, prediction_df.columns.get_loc('temp_calgary_lag16')] = alberta_weather_merged.iloc[19,0]
prediction_df.iloc[12, prediction_df.columns.get_loc('temp_calgary_lag16')] = alberta_weather_merged.iloc[20,0]
prediction_df.iloc[13, prediction_df.columns.get_loc('temp_calgary_lag16')] = alberta_weather_merged.iloc[21,0]
prediction_df.iloc[14, prediction_df.columns.get_loc('temp_calgary_lag16')] = alberta_weather_merged.iloc[22,0]
prediction_df.iloc[15, prediction_df.columns.get_loc('temp_calgary_lag16')] = alberta_weather_merged.iloc[23,0]



prediction_df.iloc[0, prediction_df.columns.get_loc('temp_calgary_lag17')] = alberta_weather_merged.iloc[7,0]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_calgary_lag17')] = alberta_weather_merged.iloc[8,0]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_calgary_lag17')] = alberta_weather_merged.iloc[9,0]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_calgary_lag17')] = alberta_weather_merged.iloc[10,0]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_calgary_lag17')] = alberta_weather_merged.iloc[11,0]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_calgary_lag17')] = alberta_weather_merged.iloc[12,0]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_calgary_lag17')] = alberta_weather_merged.iloc[13,0]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_calgary_lag17')] = alberta_weather_merged.iloc[14,0]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_calgary_lag17')] = alberta_weather_merged.iloc[15,0]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_calgary_lag17')] = alberta_weather_merged.iloc[16,0]
prediction_df.iloc[10, prediction_df.columns.get_loc('temp_calgary_lag17')] = alberta_weather_merged.iloc[17,0]
prediction_df.iloc[11, prediction_df.columns.get_loc('temp_calgary_lag17')] = alberta_weather_merged.iloc[18,0]
prediction_df.iloc[12, prediction_df.columns.get_loc('temp_calgary_lag17')] = alberta_weather_merged.iloc[19,0]
prediction_df.iloc[13, prediction_df.columns.get_loc('temp_calgary_lag17')] = alberta_weather_merged.iloc[20,0]
prediction_df.iloc[14, prediction_df.columns.get_loc('temp_calgary_lag17')] = alberta_weather_merged.iloc[21,0]
prediction_df.iloc[15, prediction_df.columns.get_loc('temp_calgary_lag17')] = alberta_weather_merged.iloc[22,0]
prediction_df.iloc[16, prediction_df.columns.get_loc('temp_calgary_lag17')] = alberta_weather_merged.iloc[23,0]



prediction_df.iloc[0, prediction_df.columns.get_loc('temp_calgary_lag18')] = alberta_weather_merged.iloc[6,0]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_calgary_lag18')] = alberta_weather_merged.iloc[7,0]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_calgary_lag18')] = alberta_weather_merged.iloc[8,0]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_calgary_lag18')] = alberta_weather_merged.iloc[9,0]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_calgary_lag18')] = alberta_weather_merged.iloc[10,0]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_calgary_lag18')] = alberta_weather_merged.iloc[11,0]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_calgary_lag18')] = alberta_weather_merged.iloc[12,0]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_calgary_lag18')] = alberta_weather_merged.iloc[13,0]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_calgary_lag18')] = alberta_weather_merged.iloc[14,0]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_calgary_lag18')] = alberta_weather_merged.iloc[15,0]
prediction_df.iloc[10, prediction_df.columns.get_loc('temp_calgary_lag18')] = alberta_weather_merged.iloc[16,0]
prediction_df.iloc[11, prediction_df.columns.get_loc('temp_calgary_lag18')] = alberta_weather_merged.iloc[17,0]
prediction_df.iloc[12, prediction_df.columns.get_loc('temp_calgary_lag18')] = alberta_weather_merged.iloc[18,0]
prediction_df.iloc[13, prediction_df.columns.get_loc('temp_calgary_lag18')] = alberta_weather_merged.iloc[19,0]
prediction_df.iloc[14, prediction_df.columns.get_loc('temp_calgary_lag18')] = alberta_weather_merged.iloc[20,0]
prediction_df.iloc[15, prediction_df.columns.get_loc('temp_calgary_lag18')] = alberta_weather_merged.iloc[21,0]
prediction_df.iloc[16, prediction_df.columns.get_loc('temp_calgary_lag18')] = alberta_weather_merged.iloc[22,0]
prediction_df.iloc[17, prediction_df.columns.get_loc('temp_calgary_lag18')] = alberta_weather_merged.iloc[23,0]




prediction_df.iloc[0, prediction_df.columns.get_loc('temp_calgary_lag19')] = alberta_weather_merged.iloc[5,0]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_calgary_lag19')] = alberta_weather_merged.iloc[6,0]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_calgary_lag19')] = alberta_weather_merged.iloc[7,0]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_calgary_lag19')] = alberta_weather_merged.iloc[8,0]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_calgary_lag19')] = alberta_weather_merged.iloc[9,0]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_calgary_lag19')] = alberta_weather_merged.iloc[10,0]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_calgary_lag19')] = alberta_weather_merged.iloc[11,0]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_calgary_lag19')] = alberta_weather_merged.iloc[12,0]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_calgary_lag19')] = alberta_weather_merged.iloc[13,0]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_calgary_lag19')] = alberta_weather_merged.iloc[14,0]
prediction_df.iloc[10, prediction_df.columns.get_loc('temp_calgary_lag19')] = alberta_weather_merged.iloc[15,0]
prediction_df.iloc[11, prediction_df.columns.get_loc('temp_calgary_lag19')] = alberta_weather_merged.iloc[16,0]
prediction_df.iloc[12, prediction_df.columns.get_loc('temp_calgary_lag19')] = alberta_weather_merged.iloc[17,0]
prediction_df.iloc[13, prediction_df.columns.get_loc('temp_calgary_lag19')] = alberta_weather_merged.iloc[18,0]
prediction_df.iloc[14, prediction_df.columns.get_loc('temp_calgary_lag19')] = alberta_weather_merged.iloc[19,0]
prediction_df.iloc[15, prediction_df.columns.get_loc('temp_calgary_lag19')] = alberta_weather_merged.iloc[20,0]
prediction_df.iloc[16, prediction_df.columns.get_loc('temp_calgary_lag19')] = alberta_weather_merged.iloc[21,0]
prediction_df.iloc[17, prediction_df.columns.get_loc('temp_calgary_lag19')] = alberta_weather_merged.iloc[22,0]
prediction_df.iloc[18, prediction_df.columns.get_loc('temp_calgary_lag19')] = alberta_weather_merged.iloc[23,0]



prediction_df.iloc[0, prediction_df.columns.get_loc('temp_calgary_lag20')] = alberta_weather_merged.iloc[4,0]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_calgary_lag20')] = alberta_weather_merged.iloc[5,0]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_calgary_lag20')] = alberta_weather_merged.iloc[6,0]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_calgary_lag20')] = alberta_weather_merged.iloc[7,0]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_calgary_lag20')] = alberta_weather_merged.iloc[8,0]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_calgary_lag20')] = alberta_weather_merged.iloc[9,0]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_calgary_lag20')] = alberta_weather_merged.iloc[10,0]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_calgary_lag20')] = alberta_weather_merged.iloc[11,0]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_calgary_lag20')] = alberta_weather_merged.iloc[12,0]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_calgary_lag20')] = alberta_weather_merged.iloc[13,0]
prediction_df.iloc[10, prediction_df.columns.get_loc('temp_calgary_lag20')] = alberta_weather_merged.iloc[14,0]
prediction_df.iloc[11, prediction_df.columns.get_loc('temp_calgary_lag20')] = alberta_weather_merged.iloc[15,0]
prediction_df.iloc[12, prediction_df.columns.get_loc('temp_calgary_lag20')] = alberta_weather_merged.iloc[16,0]
prediction_df.iloc[13, prediction_df.columns.get_loc('temp_calgary_lag20')] = alberta_weather_merged.iloc[17,0]
prediction_df.iloc[14, prediction_df.columns.get_loc('temp_calgary_lag20')] = alberta_weather_merged.iloc[18,0]
prediction_df.iloc[15, prediction_df.columns.get_loc('temp_calgary_lag20')] = alberta_weather_merged.iloc[19,0]
prediction_df.iloc[16, prediction_df.columns.get_loc('temp_calgary_lag20')] = alberta_weather_merged.iloc[20,0]
prediction_df.iloc[17, prediction_df.columns.get_loc('temp_calgary_lag20')] = alberta_weather_merged.iloc[21,0]
prediction_df.iloc[18, prediction_df.columns.get_loc('temp_calgary_lag20')] = alberta_weather_merged.iloc[22,0]
prediction_df.iloc[19, prediction_df.columns.get_loc('temp_calgary_lag20')] = alberta_weather_merged.iloc[23,0]




prediction_df.iloc[0, prediction_df.columns.get_loc('temp_calgary_lag21')] = alberta_weather_merged.iloc[3,0]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_calgary_lag21')] = alberta_weather_merged.iloc[4,0]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_calgary_lag21')] = alberta_weather_merged.iloc[5,0]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_calgary_lag21')] = alberta_weather_merged.iloc[6,0]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_calgary_lag21')] = alberta_weather_merged.iloc[7,0]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_calgary_lag21')] = alberta_weather_merged.iloc[8,0]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_calgary_lag21')] = alberta_weather_merged.iloc[9,0]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_calgary_lag21')] = alberta_weather_merged.iloc[10,0]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_calgary_lag21')] = alberta_weather_merged.iloc[11,0]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_calgary_lag21')] = alberta_weather_merged.iloc[12,0]
prediction_df.iloc[10, prediction_df.columns.get_loc('temp_calgary_lag21')] = alberta_weather_merged.iloc[13,0]
prediction_df.iloc[11, prediction_df.columns.get_loc('temp_calgary_lag21')] = alberta_weather_merged.iloc[14,0]
prediction_df.iloc[12, prediction_df.columns.get_loc('temp_calgary_lag21')] = alberta_weather_merged.iloc[15,0]
prediction_df.iloc[13, prediction_df.columns.get_loc('temp_calgary_lag21')] = alberta_weather_merged.iloc[16,0]
prediction_df.iloc[14, prediction_df.columns.get_loc('temp_calgary_lag21')] = alberta_weather_merged.iloc[17,0]
prediction_df.iloc[15, prediction_df.columns.get_loc('temp_calgary_lag21')] = alberta_weather_merged.iloc[18,0]
prediction_df.iloc[16, prediction_df.columns.get_loc('temp_calgary_lag21')] = alberta_weather_merged.iloc[19,0]
prediction_df.iloc[17, prediction_df.columns.get_loc('temp_calgary_lag21')] = alberta_weather_merged.iloc[20,0]
prediction_df.iloc[18, prediction_df.columns.get_loc('temp_calgary_lag21')] = alberta_weather_merged.iloc[21,0]
prediction_df.iloc[19, prediction_df.columns.get_loc('temp_calgary_lag21')] = alberta_weather_merged.iloc[22,0]
prediction_df.iloc[20, prediction_df.columns.get_loc('temp_calgary_lag21')] = alberta_weather_merged.iloc[23,0]



prediction_df.iloc[0, prediction_df.columns.get_loc('temp_calgary_lag22')] = alberta_weather_merged.iloc[2,0]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_calgary_lag22')] = alberta_weather_merged.iloc[3,0]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_calgary_lag22')] = alberta_weather_merged.iloc[4,0]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_calgary_lag22')] = alberta_weather_merged.iloc[5,0]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_calgary_lag22')] = alberta_weather_merged.iloc[6,0]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_calgary_lag22')] = alberta_weather_merged.iloc[7,0]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_calgary_lag22')] = alberta_weather_merged.iloc[8,0]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_calgary_lag22')] = alberta_weather_merged.iloc[9,0]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_calgary_lag22')] = alberta_weather_merged.iloc[10,0]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_calgary_lag22')] = alberta_weather_merged.iloc[11,0]
prediction_df.iloc[10, prediction_df.columns.get_loc('temp_calgary_lag22')] = alberta_weather_merged.iloc[12,0]
prediction_df.iloc[11, prediction_df.columns.get_loc('temp_calgary_lag22')] = alberta_weather_merged.iloc[13,0]
prediction_df.iloc[12, prediction_df.columns.get_loc('temp_calgary_lag22')] = alberta_weather_merged.iloc[14,0]
prediction_df.iloc[13, prediction_df.columns.get_loc('temp_calgary_lag22')] = alberta_weather_merged.iloc[15,0]
prediction_df.iloc[14, prediction_df.columns.get_loc('temp_calgary_lag22')] = alberta_weather_merged.iloc[16,0]
prediction_df.iloc[15, prediction_df.columns.get_loc('temp_calgary_lag22')] = alberta_weather_merged.iloc[17,0]
prediction_df.iloc[16, prediction_df.columns.get_loc('temp_calgary_lag22')] = alberta_weather_merged.iloc[18,0]
prediction_df.iloc[17, prediction_df.columns.get_loc('temp_calgary_lag22')] = alberta_weather_merged.iloc[19,0]
prediction_df.iloc[18, prediction_df.columns.get_loc('temp_calgary_lag22')] = alberta_weather_merged.iloc[20,0]
prediction_df.iloc[19, prediction_df.columns.get_loc('temp_calgary_lag22')] = alberta_weather_merged.iloc[21,0]
prediction_df.iloc[20, prediction_df.columns.get_loc('temp_calgary_lag22')] = alberta_weather_merged.iloc[22,0]
prediction_df.iloc[21, prediction_df.columns.get_loc('temp_calgary_lag22')] = alberta_weather_merged.iloc[23,0]



prediction_df.iloc[0, prediction_df.columns.get_loc('temp_calgary_lag23')] = alberta_weather_merged.iloc[1,0]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_calgary_lag23')] = alberta_weather_merged.iloc[2,0]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_calgary_lag23')] = alberta_weather_merged.iloc[3,0]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_calgary_lag23')] = alberta_weather_merged.iloc[4,0]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_calgary_lag23')] = alberta_weather_merged.iloc[5,0]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_calgary_lag23')] = alberta_weather_merged.iloc[6,0]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_calgary_lag23')] = alberta_weather_merged.iloc[7,0]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_calgary_lag23')] = alberta_weather_merged.iloc[8,0]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_calgary_lag23')] = alberta_weather_merged.iloc[9,0]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_calgary_lag23')] = alberta_weather_merged.iloc[10,0]
prediction_df.iloc[10, prediction_df.columns.get_loc('temp_calgary_lag23')] = alberta_weather_merged.iloc[11,0]
prediction_df.iloc[11, prediction_df.columns.get_loc('temp_calgary_lag23')] = alberta_weather_merged.iloc[12,0]
prediction_df.iloc[12, prediction_df.columns.get_loc('temp_calgary_lag23')] = alberta_weather_merged.iloc[13,0]
prediction_df.iloc[13, prediction_df.columns.get_loc('temp_calgary_lag23')] = alberta_weather_merged.iloc[14,0]
prediction_df.iloc[14, prediction_df.columns.get_loc('temp_calgary_lag23')] = alberta_weather_merged.iloc[15,0]
prediction_df.iloc[15, prediction_df.columns.get_loc('temp_calgary_lag23')] = alberta_weather_merged.iloc[16,0]
prediction_df.iloc[16, prediction_df.columns.get_loc('temp_calgary_lag23')] = alberta_weather_merged.iloc[17,0]
prediction_df.iloc[17, prediction_df.columns.get_loc('temp_calgary_lag23')] = alberta_weather_merged.iloc[18,0]
prediction_df.iloc[18, prediction_df.columns.get_loc('temp_calgary_lag23')] = alberta_weather_merged.iloc[19,0]
prediction_df.iloc[19, prediction_df.columns.get_loc('temp_calgary_lag23')] = alberta_weather_merged.iloc[20,0]
prediction_df.iloc[20, prediction_df.columns.get_loc('temp_calgary_lag23')] = alberta_weather_merged.iloc[21,0]
prediction_df.iloc[21, prediction_df.columns.get_loc('temp_calgary_lag23')] = alberta_weather_merged.iloc[22,0]
prediction_df.iloc[22, prediction_df.columns.get_loc('temp_calgary_lag23')] = alberta_weather_merged.iloc[23,0]



prediction_df.iloc[0, prediction_df.columns.get_loc('temp_calgary_lag24')] = alberta_weather_merged.iloc[0,0]
prediction_df.iloc[1, prediction_df.columns.get_loc('temp_calgary_lag24')] = alberta_weather_merged.iloc[1,0]
prediction_df.iloc[2, prediction_df.columns.get_loc('temp_calgary_lag24')] = alberta_weather_merged.iloc[2,0]
prediction_df.iloc[3, prediction_df.columns.get_loc('temp_calgary_lag24')] = alberta_weather_merged.iloc[3,0]
prediction_df.iloc[4, prediction_df.columns.get_loc('temp_calgary_lag24')] = alberta_weather_merged.iloc[4,0]
prediction_df.iloc[5, prediction_df.columns.get_loc('temp_calgary_lag24')] = alberta_weather_merged.iloc[5,0]
prediction_df.iloc[6, prediction_df.columns.get_loc('temp_calgary_lag24')] = alberta_weather_merged.iloc[6,0]
prediction_df.iloc[7, prediction_df.columns.get_loc('temp_calgary_lag24')] = alberta_weather_merged.iloc[7,0]
prediction_df.iloc[8, prediction_df.columns.get_loc('temp_calgary_lag24')] = alberta_weather_merged.iloc[8,0]
prediction_df.iloc[9, prediction_df.columns.get_loc('temp_calgary_lag24')] = alberta_weather_merged.iloc[9,0]
prediction_df.iloc[10, prediction_df.columns.get_loc('temp_calgary_lag24')] = alberta_weather_merged.iloc[10,0]
prediction_df.iloc[11, prediction_df.columns.get_loc('temp_calgary_lag24')] = alberta_weather_merged.iloc[11,0]
prediction_df.iloc[12, prediction_df.columns.get_loc('temp_calgary_lag24')] = alberta_weather_merged.iloc[12,0]
prediction_df.iloc[13, prediction_df.columns.get_loc('temp_calgary_lag24')] = alberta_weather_merged.iloc[13,0]
prediction_df.iloc[14, prediction_df.columns.get_loc('temp_calgary_lag24')] = alberta_weather_merged.iloc[14,0]
prediction_df.iloc[15, prediction_df.columns.get_loc('temp_calgary_lag24')] = alberta_weather_merged.iloc[15,0]
prediction_df.iloc[16, prediction_df.columns.get_loc('temp_calgary_lag24')] = alberta_weather_merged.iloc[16,0]
prediction_df.iloc[17, prediction_df.columns.get_loc('temp_calgary_lag24')] = alberta_weather_merged.iloc[17,0]
prediction_df.iloc[18, prediction_df.columns.get_loc('temp_calgary_lag24')] = alberta_weather_merged.iloc[18,0]
prediction_df.iloc[19, prediction_df.columns.get_loc('temp_calgary_lag24')] = alberta_weather_merged.iloc[19,0]
prediction_df.iloc[20, prediction_df.columns.get_loc('temp_calgary_lag24')] = alberta_weather_merged.iloc[20,0]
prediction_df.iloc[21, prediction_df.columns.get_loc('temp_calgary_lag24')] = alberta_weather_merged.iloc[21,0]
prediction_df.iloc[22, prediction_df.columns.get_loc('temp_calgary_lag24')] = alberta_weather_merged.iloc[22,0]
prediction_df.iloc[23, prediction_df.columns.get_loc('temp_calgary_lag24')] = alberta_weather_merged.iloc[23,0]



# type conversion for the model

prediction_df.iloc[:,20:72] = prediction_df.iloc[:,20:72].astype('float64')


#save the input data for future isnpection

prediction_df.to_csv('input_data_{}_{}_{}_generate_date_{}_v11.csv'.format(day_of_run, month_of_run, year_of_run, now_time))


#Generate the forecast


loaded_model = joblib.load("model_generated_on_17_2_2021_v11.joblib.dat")


forecast = pd.DataFrame() # this dataframe will contain the forecasts (timestamp and values)
forecast['datetime_of_forecast']= forecast_interval_stamps.values 
forecast['values']              = 0 #initializing the column with zero

for i in range(0, 24):
    input_data          = prediction_df.iloc[i]
    predictions         = loaded_model.predict(np.array(input_data).reshape((1,-1)))
    forecast.iloc[i,1]  = predictions[0]


#save the data for future isnpection
forecast.to_csv('forecast_{}_{}_{}_generate_date_{}.csv'.format(day_of_run, month_of_run, year_of_run, now_time ))


