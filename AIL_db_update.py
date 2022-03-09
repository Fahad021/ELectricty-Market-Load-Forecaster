#!/usr/bin/env python
# coding: utf-8

# In[1]:


import http.client
import json
import time
import csv
from datetime import datetime
from datetime import timedelta
import numpy as np
import pandas as pd
import sqlite3
import holidays
import re
import sys
if sys.version_info[0] < 3: 
    from StringIO import StringIO
else:
    from io import StringIO

##########################################################

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
                # Check if the response is good
                
                if res_code == 200:
                    res_data = res.read()
                    # Decode the token into an object
                    jsonData = json.loads(res_data.decode('utf-8'))
                    self.accessToken = jsonData['access_token']                         
                    # Calculate new expiry date
                    self.tokenExpiry = datetime.now() + timedelta(seconds=jsonData['expires_in'])                        
                    #print('token obtained')
                    #print(self.accessToken)
                else:
                    res_data = res.read()
                    print(res_data.decode('utf-8'))
                conn.close()                          
        except Exception as e:
            print("getToken: " + str(e))
            # Release token if an error occured
            self.releaseToken()      

    def releaseToken(self):
        try:            
            headers = {}
            headers['Authorization'] = f'Bearer {self.accessToken}'            
            conn = http.client.HTTPSConnection(self.server)
            conn.request('DELETE', self.releasePath, None, headers)  
            res = conn.getresponse()
            res_code = res.code
            if res_code == 200:   
                # Set expiration date back to guarantee isTokenValid() returns false                
                self.tokenExpiry = datetime.now() - timedelta(seconds=60)
                #print('token released')            
        except Exception as e:
            print("releaseToken: " + str(e))
                    
    def isTokenValid(self):
        if self.tokenExpiry==None:
            return False
        elif datetime.now() >= self.tokenExpiry:            
            return False
        else:
            return True            
    
    def GetStreamDataByStreamId(self,streamIds, fromDate, toDate, dataFormat='csv', dataOption=''):
        stream_data = "" 
        # Set file format to csv or json            
        DataFormats = {}
        DataFormats['csv'] = 'text/csv'
        DataFormats['json'] = 'Application/json'
        
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
                    headers = {}            
                    headers['Accept'] = DataFormats[dataFormat]
                    headers['Authorization']= f'Bearer {self.accessToken}'
                    
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
                            print(str(e))            
                            self.releaseToken()
                            return None  
                    else:
                        print(str(res_code) + " - " + str(res.reason) + " - " + str(res.read().decode('utf-8')))
                    
                self.releaseToken()   
                # Wait 1 second before next request
                time.sleep(1)
            return stream_data        
        except Exception as e:
            print(str(e))    
            self.releaseToken()
            return None
        
        
    def StreamDataOptions(self, streamId, dataFormat='csv'):
        try:      
            DataFormats = {}
            DataFormats['csv'] = 'text/csv'
            DataFormats['json'] = 'Application/json'
            resultSet = {}
            for streamId in streamIds:
                # Get an access token    
                if streamId not in resultSet:
                    self.getToken()                        
                    if self.isTokenValid():                 
                        # Setup the path for data request.
                        path = f'/api/StreamDataOptions/{streamId}'                        
                        # Create request header
                        headers = {}     
                        headers['Accept'] = DataFormats[dataFormat]                                   
                        headers['Authorization'] = f'Bearer {self.accessToken}'
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
            print(str(e))    
            self.releaseToken()
            return None          
        
        except Exception as e:            
            self.releaseToken()                        
            return str(e)

        
# Authenticate with your NRG Stream username and password    
nrgStreamApi = NRGStreamApi('Username','Password')   


# In[2]:



def run_query(q):
    with sqlite3.connect('AIL_db.db') as conn:
        return pd.read_sql_query(q, conn)

# view columns in table (or whatever you want to call with SQL syntax)
query          = "SELECT * FROM HISTORICALFCAST2"
history        = run_query(query)

#history


# In[3]:


#for i in range(0,len(history.dtypes)):
#    print(i,'--->',history.columns[i],'--->',history.dtypes[i])


# In[4]:


db_end_date        =  pd.to_datetime(history.iloc[history.shape[0]-1,0])
update_start_date  =  db_end_date + timedelta(days=1)

update_start_year  =  update_start_date.year
update_start_month =  update_start_date.month
update_start_day   =  update_start_date.day

fromDate           = '{}/{}/{}'.format(update_start_month,update_start_day,update_start_year )

now_time           = datetime.now()
year_of_run        = now_time.year
month_of_run       = now_time.month
day_of_run         = now_time.day


toDate = '{}/{}/{}'.format(month_of_run, day_of_run,year_of_run)

#print(fromDate,toDate)


# In[5]:


stream           = [3]

for i in stream:
    nrgStreamApi = NRGStreamApi('Username','Password')
    ids          = [i]
    stream_data  = nrgStreamApi.GetStreamDataByStreamId(ids, fromDate, toDate, 'csv', '')        
    STREAM_DATA  = StringIO(stream_data)
    temp_df      = pd.read_csv(STREAM_DATA, sep=";")

    
temp_df          = temp_df[14:temp_df.shape[0]]
temp_df.columns  = ["Datetime,AIL"]
new              = temp_df['Datetime,AIL'].str.split(",", n = 2, expand = True) 
# making separate first name column from new data frame 
temp_df["Datetime"] = new[0] 
# making separate last name column from new data frame 
temp_df["AIL"]      = new[1] 
temp_df['AIL']      = pd.to_numeric(temp_df['AIL'],errors='coerce')
temp_df['Datetime'] = pd.to_datetime(temp_df['Datetime'])
temp_df             = temp_df.drop(columns=['Datetime,AIL'],axis=1)
temp_df             = temp_df.reset_index(drop=True)

#temp_df


# In[6]:


#column_names = history.columns[0:91]
column_names    =  history.columns
#column_names


# In[7]:


df = pd.DataFrame(columns = column_names)


# In[8]:


df.iloc[:,0] = temp_df.iloc[:,0]
df.iloc[:,1] = temp_df.iloc[:,1]


# In[9]:


df['hour_of_day'] = df['Datetime'].dt.hour

#------------------------------

# create a list of our conditions
conditions       = [
    (df['hour_of_day'] < 7),
    (df['hour_of_day'] >= 7) & (df['hour_of_day'] <= 19),
    (df['hour_of_day'] > 19)
    ]

# create a list of the values we want to assign for each condition
values = [1, 0, 1]

# create a new column and use np.select to assign values to it using our lists as arguments
df['off_peak']   = np.select(conditions, values)

conditions       = [
    (df['hour_of_day'] < 7),
    (df['hour_of_day'] >= 7) & (df['hour_of_day'] <= 19),
    (df['hour_of_day'] > 19)
    ]

# create a list of the values we want to assign for each condition
values = [0, 1, 0]

# create a new column and use np.select to assign values to it using our lists as arguments
df['on_peak']   = np.select(conditions, values)

#----------------------
df['just_date'] = df['Datetime'].dt.date
dates           = df['just_date']
day             = pd.Series([d.timetuple().tm_yday for d in dates])
df['day']       = day

df['sin.day']   = np.sin(day*2*np.pi/365 + np.pi/4)
df['cos.day']   = np.cos(day*2*np.pi/365 + np.pi/4)
df['sin.hour']  = np.sin(df['hour_of_day']*2*np.pi/24)
df['cos.hour']  = np.cos(df['hour_of_day']*2*np.pi/24)
#-------------------------

weekdays        = [d.weekday() for d in dates]
df['weekend']   = [1 if d >= 5 else 0 for d in weekdays]
for i, s in enumerate(['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday','sunday']):
    df[s] = [1 if d == i else 0 for d in weekdays]


# In[10]:


number_of_month  = pd.to_datetime(fromDate).month
for i in range(19,19+12):
    if ((int(number_of_month)-1)+16) == i:
        df.iloc[:,i] = 1
    else:
        df.iloc[:,i] = 0
#df.iloc[:,19:(19+12)]


# In[11]:


for i in range(24):
    df['hour_%d' % i] = np.where(df['hour_of_day']==i, 1, 0)
#df


# In[12]:


xls        = pd.ExcelFile('edmonton_sunrise_sunset.xls')
dfess_2015 = pd.read_excel(xls, '2015') #df= dafaframe, e= edmonston, ss= sunrise and sunset 
dfess_2016 = pd.read_excel(xls, '2016')

df['year'] = df['Datetime'].dt.year
dfess_2015 = dfess_2015[['Sunrise_hr', 'Sunset_hr']]
dfess_2016 = dfess_2016[['Sunrise_hr', 'Sunset_hr']]
df['sunlight_avaialbility']           =  ''
df['sunlight_avaialbility'].values[:] = 0
for i in range(0,df.shape[0]):
    if df.iloc[i,df.columns.get_loc('year')] == 2016:
        criteria =  df.columns.get_loc('day') #day_of_year
        sunrise  = dfess_2016.iloc[criteria-1,0] #sunrise
        sunset   = dfess_2016.iloc[criteria-1,1] #sunset
        if (df.iloc[i,2]>= sunrise) and (df.iloc[i,2] <= sunset):
            df.iloc[i,df.columns.get_loc('sunlight_avaialbility')] = 1
    else:
        criteria = df.columns.get_loc('day')#day_of_year
        sunrise  = dfess_2015.iloc[criteria-1,0]#sunrise
        sunset   = dfess_2015.iloc[criteria-1,1] #sunset
        if (df.iloc[i,2]>= sunrise) and (df.iloc[i,2] <= sunset):
            df.iloc[i, df.columns.get_loc('sunlight_avaialbility')] = 1
#df


# In[13]:


df['AIL_previous_hour']   = df['AIL'].shift(1) # col_index = 57
df['AIL_24h_lagged']      = df['AIL'].shift(24)  # col_index = 58
df['AIL_oneweek_lagged']  = df['AIL'].shift(24*7) #col_index = 59


# In[14]:



hl_list = holidays.CA(years=[2021], prov = 'AB').items()
df_hl   = pd.DataFrame(hl_list)
df_hl.columns =['date','title']
df_hl['date'] =pd.to_datetime(df_hl.date)
df_hl = df_hl.sort_values('date')
df['holiday'] = [1 if d in df_hl['date'] else 0 for d in df['hour_of_day']]
#df


# In[15]:


df['monday_holiday']    = df['monday']    *  df['holiday']
df['tuesday_holiday']   = df['tuesday']   *  df['holiday'] 
df['wednesday_holiday'] = df['wednesday'] *  df['holiday']
df['thursday_holiday']  = df['thursday']  *  df['holiday']
df['friday_holiday']    = df['friday']    *  df['holiday']
df['weekend_holiday']   = df['weekend']   *  df['holiday']

df['month0_mondayholiday']  = df['month_0']  * df['monday_holiday']
df['month1_mondayholiday']  = df['month_1']  * df['monday_holiday']
df['month2_mondayholiday']  = df['month_2']  * df['monday_holiday']
df['month3_mondayholiday']  = df['month_3']  * df['monday_holiday']
df['month4_mondayholiday']  = df['month_4']  * df['monday_holiday']
df['month5_mondayholiday']  = df['month_5']  * df['monday_holiday']
df['month6_mondayholiday']  = df['month_6']  * df['monday_holiday']
df['month7_mondayholiday']  = df['month_7']  * df['monday_holiday']
df['month8_mondayholiday']  = df['month_8']  * df['monday_holiday']
df['month9_mondayholiday']  = df['month_9']  * df['monday_holiday']
df['month10_mondayholiday'] = df['month_10'] * df['monday_holiday']
df['month11_mondayholiday'] = df['month_11'] * df['monday_holiday']

df['month0_fridayholiday']  = df['month_0']  * df['friday_holiday']
df['month1_fridayholiday']  = df['month_1']  * df['friday_holiday']
df['month2_fridayholiday']  = df['month_2']  * df['friday_holiday']
df['month3_fridayholiday']  = df['month_3']  * df['friday_holiday']
df['month4_fridayholiday']  = df['month_4']  * df['friday_holiday']
df['month5_fridayholiday']  = df['month_5']  * df['friday_holiday']
df['month6_fridayholiday']  = df['month_6']  * df['friday_holiday']
df['month7_fridayholiday']  = df['month_7']  * df['friday_holiday']
df['month8_fridayholiday']  = df['month_8']  * df['friday_holiday']
df['month9_fridayholiday']  = df['month_9']  * df['friday_holiday']
df['month10_fridayholiday'] = df['month_10']* df['friday_holiday']
df['month11_fridayholiday'] = df['month_11']* df['friday_holiday']

df['sunlight_mondayholiday']=  df['sunlight_avaialbility']* df['monday_holiday']
df['sunlight_tuesdayholiday']= df['sunlight_avaialbility']* df['tuesday_holiday']
#df['sunlight_tuesdayholiday'] = df['sunlight_avaialbility']*df['wednesday_holiday']
#df['sunlight_tuesdayholiday'] = df['sunlight_avaialbility']*df['thursday_holiday']
#df['sunlight_tuesdayholiday'] = df['sunlight_avaialbility']*df['friday_holiday']

#df


# In[16]:


df.iloc[0,df.columns.get_loc('AIL_previous_hour')] = history.iloc[history.shape[0]-1,history.columns.get_loc('AIL')]


# In[17]:


df.iloc[0:24,df.columns.get_loc('AIL_24h_lagged')] = history.iloc[(history.shape[0]-24): (history.shape[0]),history.columns.get_loc('AIL')]


# In[18]:


df.iloc[0:24*7,df.columns.get_loc('AIL_oneweek_lagged')] = history.iloc[(history.shape[0]-(24*7)): (history.shape[0]),history.columns.get_loc('AIL')]


# In[19]:


#df.columns


# In[20]:


#history.columns


# In[21]:

#conn = sqlite3.connect('AIL_db.db')
df.to_sql('HISTORICALFCAST2', con=sqlite3.connect('AIL_db.db'), if_exists='append', index = False)


# In[22]:


#def run_query(q):
#    with sqlite3.connect('AIL_db.db') as conn:
#        return pd.read_sql_query(q, conn)

# view columns in table (or whatever you want to call with SQL syntax)
#query = "SELECT * FROM HISTORICALFCAST2"
#history = run_query(query)
#history


# In[ ]:




