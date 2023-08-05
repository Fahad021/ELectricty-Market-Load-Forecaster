#!/usr/bin/env python
# coding: utf-8

# In[4]:


import http.client
import json
import time
import csv
from   datetime import datetime
from   datetime import timedelta
import numpy as np
import pandas as pd
import sqlite3
import re
import sys
if sys.version_info[0] < 3: 
    from StringIO import StringIO
else:
    from io import StringIO


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


# In[3]:


def run_query(q):
    with sqlite3.connect('weather_db.db') as conn:
        return pd.read_sql_query(q, conn)

# view columns in table (or whatever you want to call with SQL syntax)
query = "SELECT * FROM HISTORICALFCAST"
df = run_query(query)
df


# In[5]:


df.dtypes


# In[11]:


db_end_year  = pd.to_datetime(df.iloc[df.shape[0]-1,0]).year
db_end_month = pd.to_datetime(df.iloc[df.shape[0]-1,0]).month
db_end_date  =  pd.to_datetime(df.iloc[df.shape[0]-1,0]).day

#print(db_end_year,db_end_month,db_end_date)


fromDate = f'{db_end_month}/{db_end_date + 1}/{db_end_year}'

now_time     = datetime.now()
year_of_run  = now_time.year
month_of_run = now_time.month
day_of_run   = now_time.day


toDate = f'{month_of_run}/{day_of_run}/{year_of_run}'

#print(fromDate, toDate)


# In[15]:


df_clgry   = pd.DataFrame()
df_edmtn   = pd.DataFrame()
df_ftmcmry = pd.DataFrame()
df_lthbrg  = pd.DataFrame()
df_mdcnht  = pd.DataFrame()
df_rddr    = pd.DataFrame()
df_slvlk   = pd.DataFrame()
df_list    = {}

streams = [242498, 242497, 242500, 242508, 242511, 242519, 242522]
city_names = ['calgary', 'edmonton', 'ftmcmry','lthbrg','mdcnht', 'rddr','slvlk']
k = 0

for i in streams:
    stream_data     = nrgStreamApi.GetStreamDataByStreamId([i], fromDate, toDate, 'csv', '')
    STREAM_DATA     = StringIO(stream_data)
    df              = pd.read_csv(STREAM_DATA, sep=";")
    length          = df.shape[0]
    df              = df[16:length] # removing header information
    df.columns      = ["Datetime,temp,wind,direction"]
    new             = df['Datetime,temp,wind,direction'].str.split(",", n = 4, expand = True)
    # making separate first name column from new data frame 
    df["Datetime"]  = new[0]
    df["temp"]      = new[1]
    df["wind"]      = new [2]
    df["direction"] = new[3]
    df              = df.drop(['Datetime,temp,wind,direction','direction'],axis=1)
    df              = df.reset_index(drop=True)
    df.columns = [f'{str(col)}_' + city_names[k] for col in df.columns]
    df_list[k]      = df
    k               = k+1

df_clgry   = df_list[0]
df_edmtn   = df_list[1]
df_ftmcmry = df_list[2]
df_lthbrg  = df_list[3]
df_mdcnht  = df_list[4]
df_rddr    = df_list[5]
df_slvlk   = df_list[6]


alberta_weather_merged = pd.concat([df_clgry, df_edmtn, df_ftmcmry, 
                                   df_lthbrg, df_mdcnht,
                                   df_rddr, df_slvlk], axis=1)

alberta_weather_merged = alberta_weather_merged[['Datetime_calgary', 'temp_calgary', 'wind_calgary',
       'temp_edmonton', 'wind_edmonton', 'temp_ftmcmry', 'wind_ftmcmry',
       'temp_lthbrg', 'wind_lthbrg', 'temp_mdcnht', 'wind_mdcnht', 'temp_rddr',
       'wind_rddr', 'temp_slvlk', 'wind_slvlk']]

#alberta_weather_merged


# In[16]:


#alberta_weather_merged.columns


# In[19]:



conn = sqlite3.connect('weather_db.db')
c = conn.cursor()
c.executemany(
    '''
    INSERT INTO HISTORICALFCAST(Datetime_calgary, temp_calgary, wind_calgary, temp_edmonton,
    wind_edmonton, temp_ftmcmry, wind_ftmcmry, temp_lthbrg,
    wind_lthbrg, temp_mdcnht, wind_mdcnht, temp_rddr, wind_rddr,
    temp_slvlk, wind_slvlk) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ''', alberta_weather_merged.values)

conn.commit()


# In[20]:


#def run_query(q):
#    with sqlite3.connect('weather_db.db') as conn:
#        return pd.read_sql_query(q, conn)

# view columns in table (or whatever you want to call with SQL syntax)
#query = "SELECT * FROM HISTORICALFCAST"
#df = run_query(query)
#df


# In[21]:


#df.dtypes


# In[22]:


c.close()
conn.close()


# In[ ]:




