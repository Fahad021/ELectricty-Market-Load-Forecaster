#!/usr/bin/env python
# coding: utf-8

# In[1]:



import sqlite3
import pickle
import joblib
import pandas  as pd
import numpy                   as      np
from   datetime                import  datetime
from   sklearn.ensemble        import  GradientBoostingRegressor
from   xgboost                 import  XGBRegressor
from   sklearn.model_selection import  TimeSeriesSplit
from   matplotlib              import  pyplot as plt
from   sklearn.model_selection import  train_test_split
from   sklearn.preprocessing   import  MinMaxScaler
from   sklearn.metrics         import  mean_squared_error, r2_score


def run_query(q, db_name):
    with sqlite3.connect(db_name) as conn:
    	query  = "SELECT * FROM HISTORICALFCAST2"

query1 = "SELECT * FROM HISTORICALFCAST2"
#database_name_1 = 'AIL_db.db'
df_non_weather = run_query(q = query1, db_name = 'AIL_db.db')
df_non_weather['Datetime'] = pd.to_datetime(df_non_weather['Datetime'],format='%d%b%Y:%H:%M:%S.%f')
#df_non_weather

query2         = "SELECT * FROM HISTORICALFCAST"
#database_name = 'weather_db.db'
df_weather    = run_query(q = query2, db_name = 'weather_db.db' )
df_weather.columns = ['Datetime', 'temp_calgary', 'wind_calgary','temp_edmonton', 'wind_edmonton', 'temp_ftmcmry', 'wind_ftmcmry', 'temp_lthbrg', 'wind_lthbrg', 'temp_mdcnht', 'wind_mdcnht', 'temp_rddr', 'wind_rddr', 'temp_slvlk', 'wind_slvlk']

df_weather['Datetime'] = pd.to_datetime(df_weather['Datetime'],format='%d%b%Y:%H:%M:%S.%f')


training_dataframe = pd.merge(df_non_weather, df_weather, on="Datetime")

df = pd.DataFrame()
df = training_dataframe
#df


'''
0 ---> Datetime ---> datetime64[ns]
1 ---> AIL ---> float64
2 ---> hour_of_day ---> int64
3 ---> off_peak ---> int64
4 ---> on_peak ---> int64
5 ---> day ---> int64
6 ---> sin.day ---> float64
7 ---> cos.day ---> float64
8 ---> sin.hour ---> float64
9 ---> cos.hour ---> float64
10 ---> weekend ---> int64
11 ---> monday ---> int64
12 ---> tuesday ---> int64
13 ---> wednesday ---> int64
14 ---> thursday ---> int64
15 ---> friday ---> int64
16 ---> saturday ---> int64
17 ---> sunday ---> int64
18 ---> month_0 ---> float64
19 ---> month_1 ---> int64
20 ---> month_2 ---> int64
21 ---> month_3 ---> int64
22 ---> month_4 ---> int64
23 ---> month_5 ---> int64
24 ---> month_6 ---> int64
25 ---> month_7 ---> int64
26 ---> month_8 ---> int64
27 ---> month_9 ---> int64
28 ---> month_10 ---> int64
29 ---> month_11 ---> int64
30 ---> hour_0 ---> int64
31 ---> hour_1 ---> int64
32 ---> hour_2 ---> int64
33 ---> hour_3 ---> int64
34 ---> hour_4 ---> int64
35 ---> hour_5 ---> int64
36 ---> hour_6 ---> int64
37 ---> hour_7 ---> int64
38 ---> hour_8 ---> int64
39 ---> hour_9 ---> int64
40 ---> hour_10 ---> int64
41 ---> hour_11 ---> int64
42 ---> hour_12 ---> int64
43 ---> hour_13 ---> int64
44 ---> hour_14 ---> int64
45 ---> hour_15 ---> int64
46 ---> hour_16 ---> int64
47 ---> hour_17 ---> int64
48 ---> hour_18 ---> int64
49 ---> hour_19 ---> int64
50 ---> hour_20 ---> int64
51 ---> hour_21 ---> int64
52 ---> hour_22 ---> int64
53 ---> hour_23 ---> int64
54 ---> year ---> int64
55 ---> sunlight_avaialbility ---> int64
56 ---> AIL_previous_hour ---> float64
57 ---> AIL_24h_lagged ---> float64
58 ---> AIL_oneweek_lagged ---> float64
59 ---> holiday ---> int64
60 ---> monday_holiday ---> int64
61 ---> tuesday_holiday ---> int64
62 ---> wednesday_holiday ---> int64
63 ---> thursday_holiday ---> int64
64 ---> friday_holiday ---> int64
65 ---> weekend_holiday ---> int64
66 ---> month0_mondayholiday ---> float64
67 ---> month1_mondayholiday ---> int64
68 ---> month2_mondayholiday ---> int64
69 ---> month3_mondayholiday ---> int64
70 ---> month4_mondayholiday ---> int64
71 ---> month5_mondayholiday ---> int64
72 ---> month6_mondayholiday ---> int64
73 ---> month7_mondayholiday ---> int64
74 ---> month8_mondayholiday ---> int64
75 ---> month9_mondayholiday ---> int64
76 ---> month10_mondayholiday ---> int64
77 ---> month11_mondayholiday ---> int64
78 ---> month0_fridayholiday ---> float64
79 ---> month1_fridayholiday ---> int64
80 ---> month2_fridayholiday ---> int64
81 ---> month3_fridayholiday ---> int64
82 ---> month4_fridayholiday ---> int64
83 ---> month5_fridayholiday ---> int64
84 ---> month6_fridayholiday ---> int64
85 ---> month7_fridayholiday ---> int64
86 ---> month8_fridayholiday ---> int64
87 ---> month9_fridayholiday ---> int64
88 ---> month10_fridayholiday ---> int64
89 ---> month11_fridayholiday ---> int64
90 ---> sunlight_mondayholiday ---> int64
91 ---> sunlight_tuesdayholiday ---> int64
92 ---> just_date ---> object
93 ---> temp_calgary ---> float64
94 ---> wind_calgary ---> float64
95 ---> temp_edmonton ---> float64
96 ---> wind_edmonton ---> float64
97 ---> temp_ftmcmry ---> float64
98 ---> wind_ftmcmry ---> float64
99 ---> temp_lthbrg ---> float64
100 ---> wind_lthbrg ---> float64
101 ---> temp_mdcnht ---> float64
102 ---> wind_mdcnht ---> float64
103 ---> temp_rddr ---> float64
104 ---> wind_rddr ---> float64
105 ---> temp_slvlk ---> float64
106 ---> wind_slvlk ---> float64
107 ---> temp_calgary_squared ---> float64
108 ---> temp_edmonton_squared ---> float64
109 ---> temp_ftmcmry_squared ---> float64
110 ---> temp_lthbrg_squared ---> float64
111 ---> temp_mdcnht_squared ---> float64
112 ---> temp_rddr_squared ---> float64
113 ---> temp_slvlk_squared ---> float64
114 ---> temp_calgary_cube ---> float64
115 ---> temp_edmonton_cube ---> float64
116 ---> temp_ftmcmry_cube ---> float64
117 ---> temp_lthbrg_cube ---> float64
118 ---> temp_mdcnht_cube ---> float64
119 ---> temp_rddr_cube ---> float64
120 ---> temp_slvlk_cube ---> float64
121 ---> wind_chill_index_calgary ---> float64
122 ---> wind_chill_index_edmonton ---> float64
123 ---> wind_chill_index_ftmcmry ---> float64
124 ---> wind_chill_index_lthbrg ---> float64
125 ---> wind_chill_index_mdcnht ---> float64
126 ---> wind_chill_index_rddr ---> float64
127 ---> wind_chill_index_slvlk ---> float64
128 ---> average_temp ---> float64
129 ---> average_temp_squared ---> float64
130 ---> average_temp_cube ---> float64
131 ---> month0_temp_avg ---> float64
132 ---> month1_temp_avg ---> float64
133 ---> month2_temp_avg ---> float64
134 ---> month3_temp_avg ---> float64
135 ---> month4_temp_avg ---> float64
136 ---> month5_temp_avg ---> float64
137 ---> month6_temp_avg ---> float64
138 ---> month7_temp_avg ---> float64
139 ---> month8_temp_avg ---> float64
140 ---> month9_temp_avg ---> float64
141 ---> month10_temp_avg ---> float64
142 ---> month11_temp_avg ---> float64
143 ---> month0_temp_avg_sqrd ---> float64
144 ---> month1_temp_avg_sqrd ---> float64
145 ---> month2_temp_avg_sqrd ---> float64
146 ---> month3_temp_avg_sqrd ---> float64
147 ---> month4_temp_avg_sqrd ---> float64
148 ---> month5_temp_avg_sqrd ---> float64
149 ---> month6_temp_avg_sqrd ---> float64
150 ---> month7_temp_avg_sqrd ---> float64
151 ---> month8_temp_avg_sqrd ---> float64
152 ---> month9_temp_avg_sqrd ---> float64
153 ---> month10_temp_avg_sqrd ---> float64
154 ---> month11_temp_avg_sqrd ---> float64
155 ---> month0_temp_avg_cubed ---> float64
156 ---> month1_temp_avg_cubed ---> float64
157 ---> month2_temp_avg_cubed ---> float64
158 ---> month3_temp_avg_cubed ---> float64
159 ---> month4_temp_avg_cubed ---> float64
160 ---> month5_temp_avg_cubed ---> float64
161 ---> month6_temp_avg_cubed ---> float64
162 ---> month7_temp_avg_cubed ---> float64
163 ---> month8_temp_avg_cubed ---> float64
164 ---> month9_temp_avg_cubed ---> float64
165 ---> month10_temp_avg_cubed ---> float64
166 ---> month11_temp_avg_cubed ---> float64
167 ---> h0_temp_avg_sqrd ---> float64
168 ---> h1_temp_avg_sqrd ---> float64
169 ---> h2_temp_avg_sqrd ---> float64
170 ---> h3_temp_avg_sqrd ---> float64
171 ---> h4_temp_avg_sqrd ---> float64
172 ---> h5_temp_avg_sqrd ---> float64
173 ---> h6_temp_avg_sqrd ---> float64
174 ---> h7_temp_avg_sqrd ---> float64
175 ---> h8_temp_avg_sqrd ---> float64
176 ---> h9_temp_avg_sqrd ---> float64
177 ---> h10_temp_avg_sqrd ---> float64
178 ---> h11_temp_avg_sqrd ---> float64
179 ---> h12_temp_avg_sqrd ---> float64
180 ---> h13_temp_avg_sqrd ---> float64
181 ---> h14_temp_avg_sqrd ---> float64
182 ---> h15_temp_avg_sqrd ---> float64
183 ---> h16_temp_avg_sqrd ---> float64
184 ---> h17_temp_avg_sqrd ---> float64
185 ---> h18_temp_avg_sqrd ---> float64
186 ---> h19_temp_avg_sqrd ---> float64
187 ---> h20_temp_avg_sqrd ---> float64
188 ---> h21_temp_avg_sqrd ---> float64
189 ---> h22_temp_avg_sqrd ---> float64
190 ---> h23_temp_avg_sqrd ---> float64
191 ---> h0_temp_avg_cbd ---> float64
192 ---> h1_temp_avg_cbd ---> float64
193 ---> h2_temp_avg_cbd ---> float64
194 ---> h3_temp_avg_cbd ---> float64
195 ---> h4_temp_avg_cbd ---> float64
196 ---> h5_temp_avg_cbd ---> float64
197 ---> h6_temp_avg_cbd ---> float64
198 ---> h7_temp_avg_cbd ---> float64
199 ---> h8_temp_avg_cbd ---> float64
200 ---> h9_temp_avg_cbd ---> float64
201 ---> h10_temp_avg_cbd ---> float64
202 ---> h11_temp_avg_cbd ---> float64
203 ---> h12_temp_avg_cbd ---> float64
204 ---> h13_temp_avg_cbd ---> float64
205 ---> h14_temp_avg_cbd ---> float64
206 ---> h15_temp_avg_cbd ---> float64
207 ---> h16_temp_avg_cbd ---> float64
208 ---> h17_temp_avg_cbd ---> float64
209 ---> h18_temp_avg_cbd ---> float64
210 ---> h19_temp_avg_cbd ---> float64
211 ---> h20_temp_avg_cbd ---> float64
212 ---> h21_temp_avg_cbd ---> float64
213 ---> h22_temp_avg_cbd ---> float64
214 ---> h23_temp_avg_cbd ---> float64
215 ---> month0_temp_calgary ---> float64
216 ---> month1_temp_calgary ---> float64
217 ---> month2_temp_calgary ---> float64
218 ---> month3_temp_calgary ---> float64
219 ---> month4_temp_calgary ---> float64
220 ---> month5_temp_calgary ---> float64
221 ---> month6_temp_calgary ---> float64
222 ---> month7_temp_calgary ---> float64
223 ---> month8_temp_calgary ---> float64
224 ---> month9_temp_calgary ---> float64
225 ---> month10_temp_calgary ---> float64
226 ---> month11_temp_calgary ---> float64
227 ---> month0_temp_calgary_squared ---> float64
228 ---> month1_temp_calgary_squared ---> float64
229 ---> month2_temp_calgary_squared ---> float64
230 ---> month3_temp_calgary_squared ---> float64
231 ---> month4_temp_calgary_squared ---> float64
232 ---> month5_temp_calgary_squared ---> float64
233 ---> month6_temp_calgary_squared ---> float64
234 ---> month7_temp_calgary_squared ---> float64
235 ---> month8_temp_calgary_squared ---> float64
236 ---> month9_temp_calgary_squared ---> float64
237 ---> month10_temp_calgary_squared ---> float64
238 ---> month11_temp_calgary_squared ---> float64
239 ---> month0_temp_calgary_cube ---> float64
240 ---> month1_temp_calgary_cube ---> float64
241 ---> month2_temp_calgary_cube ---> float64
242 ---> month3_temp_calgary_cube ---> float64
243 ---> month4_temp_calgary_cube ---> float64
244 ---> month5_temp_calgary_cube ---> float64
245 ---> month6_temp_calgary_cube ---> float64
246 ---> month7_temp_calgary_cube ---> float64
247 ---> month8_temp_calgary_cube ---> float64
248 ---> month9_temp_calgary_cube ---> float64
249 ---> month10_temp_calgary_cube ---> float64
250 ---> month11_temp_calgary_cube ---> float64
251 ---> hour0_temp_calgary_squared ---> float64
252 ---> hour1_temp_calgary_squared ---> float64
253 ---> hour2_temp_calgary_squared ---> float64
254 ---> hour3_temp_calgary_squared ---> float64
255 ---> hour4_temp_calgary_squared ---> float64
256 ---> hour5_temp_calgary_squared ---> float64
257 ---> hour6_temp_calgary_squared ---> float64
258 ---> hour7_temp_calgary_squared ---> float64
259 ---> hour8_temp_calgary_squared ---> float64
260 ---> hour9_temp_calgary_squared ---> float64
261 ---> hour10_temp_calgary_squared ---> float64
262 ---> hour11_temp_calgary_squared ---> float64
263 ---> hour12_temp_calgary_squared ---> float64
264 ---> hour13_temp_calgary_squared ---> float64
265 ---> hour14_temp_calgary_squared ---> float64
266 ---> hour15_temp_calgary_squared ---> float64
267 ---> hour16_temp_calgary_squared ---> float64
268 ---> hour17_temp_calgary_squared ---> float64
269 ---> hour18_temp_calgary_squared ---> float64
270 ---> hour19_temp_calgary_squared ---> float64
271 ---> hour20_temp_calgary_squared ---> float64
272 ---> hour21_temp_calgary_squared ---> float64
273 ---> hour22_temp_calgary_squared ---> float64
274 ---> hour23_temp_calgary_squared ---> float64
275 ---> hour0_temp_calgary_cube ---> float64
276 ---> hour1_temp_calgary_cube ---> float64
277 ---> hour2_temp_calgary_cube ---> float64
278 ---> hour3_temp_calgary_cube ---> float64
279 ---> hour4_temp_calgary_cube ---> float64
280 ---> hour5_temp_calgary_cube ---> float64
281 ---> hour6_temp_calgary_cube ---> float64
282 ---> hour7_temp_calgary_cube ---> float64
283 ---> hour8_temp_calgary_cube ---> float64
284 ---> hour9_temp_calgary_cube ---> float64
285 ---> hour10_temp_calgary_cube ---> float64
286 ---> hour11_temp_calgary_cube ---> float64
287 ---> hour12_temp_calgary_cube ---> float64
288 ---> hour13_temp_calgary_cube ---> float64
289 ---> hour14_temp_calgary_cube ---> float64
290 ---> hour15_temp_calgary_cube ---> float64
291 ---> hour16_temp_calgary_cube ---> float64
292 ---> hour17_temp_calgary_cube ---> float64
293 ---> hour18_temp_calgary_cube ---> float64
294 ---> hour19_temp_calgary_cube ---> float64
295 ---> hour20_temp_calgary_cube ---> float64
296 ---> hour21_temp_calgary_cube ---> float64
297 ---> hour22_temp_calgary_cube ---> float64
298 ---> hour23_temp_calgary_cube ---> float64
299 ---> month0_temp_edmonton ---> float64
300 ---> month1_temp_edmonton ---> float64
301 ---> month2_temp_edmonton ---> float64
302 ---> month3_temp_edmonton ---> float64
303 ---> month4_temp_edmonton ---> float64
304 ---> month5_temp_edmonton ---> float64
305 ---> month6_temp_edmonton ---> float64
306 ---> month7_temp_edmonton ---> float64
307 ---> month8_temp_edmonton ---> float64
308 ---> month9_temp_edmonton ---> float64
309 ---> month10_temp_edmonton ---> float64
310 ---> month11_temp_edmonton ---> float64
311 ---> month0_temp_edmonton_squared ---> float64
312 ---> month1_temp_edmonton_squared ---> float64
313 ---> month2_temp_edmonton_squared ---> float64
314 ---> month3_temp_edmonton_squared ---> float64
315 ---> month4_temp_edmonton_squared ---> float64
316 ---> month5_temp_edmonton_squared ---> float64
317 ---> month6_temp_edmonton_squared ---> float64
318 ---> month7_temp_edmonton_squared ---> float64
319 ---> month8_temp_edmonton_squared ---> float64
320 ---> month9_temp_edmonton_squared ---> float64
321 ---> month10_temp_edmonton_squared ---> float64
322 ---> month11_temp_edmonton_squared ---> float64
323 ---> month0_temp_ftmcmry ---> float64
324 ---> month1_temp_ftmcmry ---> float64
325 ---> month2_temp_ftmcmry ---> float64
326 ---> month3_temp_ftmcmry ---> float64
327 ---> month4_temp_ftmcmry ---> float64
328 ---> month5_temp_ftmcmry ---> float64
329 ---> month6_temp_ftmcmry ---> float64
330 ---> month7_temp_ftmcmry ---> float64
331 ---> month8_temp_ftmcmry ---> float64
332 ---> month9_temp_ftmcmry ---> float64
333 ---> month10_temp_ftmcmry ---> float64
334 ---> month11_temp_ftmcmry ---> float64
335 ---> month0_temp_lthbrg ---> float64
336 ---> month1_temp_lthbrg ---> float64
337 ---> month2_temp_lthbrg ---> float64
338 ---> month3_temp_lthbrg ---> float64
339 ---> month4_temp_lthbrg ---> float64
340 ---> month5_temp_lthbrg ---> float64
341 ---> month6_temp_lthbrg ---> float64
342 ---> month7_temp_lthbrg ---> float64
343 ---> month8_temp_lthbrg ---> float64
344 ---> month9_temp_lthbrg ---> float64
345 ---> month10_temp_lthbrg ---> float64
346 ---> month11_temp_lthbrg ---> float64
'''

df.iloc[:,93]      = df.iloc[:,93].astype('float')
df['wind_calgary'] = pd.to_numeric(df['wind_calgary'] )
df.iloc[:,95]      = df.iloc[:,95].astype('float')
df.iloc[:,96]      = df.iloc[:,96].astype('float')
df.iloc[:,97]      = df.iloc[:,97].astype('float')
df.iloc[:,98]      = df.iloc[:,98].astype('float')
df.iloc[:,99]      = df.iloc[:,99].astype('float')
df['wind_lthbrg']  = pd.to_numeric(df['wind_lthbrg'])
df.iloc[:,101]     = df.iloc[:,101].astype('float')
df.iloc[:,102]     = df.iloc[:,102].astype('float')
df.iloc[:,103]     = df.iloc[:,103].astype('float')
df.iloc[:,104]     = df.iloc[:,104].astype('float')
df.iloc[:,105]     = df.iloc[:,105].astype('float')
df.iloc[:,106]     = pd.to_numeric(df.iloc[:,106])


# In[16]:


df['temp_calgary_squared']  = df['temp_calgary']  * df['temp_calgary']
df['temp_edmonton_squared'] = df['temp_edmonton'] * df['temp_edmonton']
df['temp_ftmcmry_squared']  = df['temp_ftmcmry']  * df['temp_ftmcmry']
df['temp_lthbrg_squared']   = df['temp_lthbrg']   * df['temp_lthbrg']
df['temp_mdcnht_squared']   = df['temp_mdcnht']   * df['temp_mdcnht']
df['temp_rddr_squared']     = df['temp_rddr']     * df['temp_rddr']
df['temp_slvlk_squared']    = df['temp_slvlk']    * df['temp_slvlk']

df['temp_calgary_cube']     = df['temp_calgary_squared']  * df['temp_calgary']
df['temp_edmonton_cube']    = df['temp_edmonton_squared'] * df['temp_edmonton']
df['temp_ftmcmry_cube']     = df['temp_ftmcmry_squared']  * df['temp_ftmcmry']
df['temp_lthbrg_cube']      = df['temp_lthbrg_squared']   * df['temp_lthbrg']
df['temp_mdcnht_cube']      = df['temp_mdcnht_squared']   * df['temp_mdcnht']
df['temp_rddr_cube']        = df['temp_rddr_squared']     * df['temp_rddr']
df['temp_slvlk_cube']       = df['temp_slvlk_squared']    * df['temp_slvlk']

df["wind_chill_index_calgary"]  = 13.12 + 0.6215 * df['temp_calgary']  - 11.37 * (df['wind_calgary']**0.16)  + 0.3965 * df['temp_calgary']  * (df['wind_calgary']**0.16)
df["wind_chill_index_edmonton"] = 13.12 + 0.6215 * df['temp_edmonton'] - 11.37 * (df['wind_edmonton']**0.16) + 0.3965 * df['temp_edmonton'] * (df['wind_edmonton']**0.16)
df["wind_chill_index_ftmcmry"]  = 13.12 + 0.6215 * df['temp_ftmcmry']  - 11.37 * (df['wind_ftmcmry']**0.16)  + 0.3965 * df['temp_ftmcmry']  * (df['wind_ftmcmry']**0.16)
df["wind_chill_index_lthbrg"]   = 13.12 + 0.6215 * df['temp_lthbrg']   - 11.37 * (df['wind_lthbrg']**0.16)   + 0.3965 * df['temp_lthbrg']   * (df['wind_lthbrg']**0.16)
df["wind_chill_index_mdcnht"]   = 13.12 + 0.6215 * df['temp_mdcnht']   - 11.37 * (df['wind_mdcnht']**0.16)   + 0.3965 * df['temp_mdcnht']   * (df['wind_mdcnht']**0.16)
df["wind_chill_index_rddr"]     = 13.12 + 0.6215 * df['temp_rddr']     - 11.37 * (df['wind_rddr']**0.16)     + 0.3965 * df['temp_rddr']     * (df['wind_rddr']**0.16)
df["wind_chill_index_slvlk"]    = 13.12 + 0.6215 * df['temp_slvlk']    - 11.37 * (df['wind_slvlk']**0.16)    + 0.3965 * df['temp_slvlk']    * (df['wind_slvlk']**0.16)

#df['MA_tempcalgary'] = df['temp_calgary'].rolling(window=24).mean()
    
#df['MA_tempcalgary'] = df['temp_calgary'].rolling(window=24).mean()
#df['MA_tempedmonton'] = df['temp_edmonton'].rolling(window=24).mean()
#df['MA_tempftmcmry'] = df['temp_ftmcmry'].rolling(window=24).mean()
#df['MA_tempmdcnht'] = df['temp_mdcnht'].rolling(window=24).mean()
#df['MA_temprddr'] = df['temp_rddr'].rolling(window=24).mean()
#df['MA_tempslvlk'] = df['temp_slvlk'].rolling(window=24).mean()

df['average_temp']         = df[['temp_calgary', 'temp_edmonton', 'temp_ftmcmry','temp_lthbrg','temp_mdcnht', 'temp_rddr','temp_slvlk']].mean(axis=1)
df['average_temp_squared'] = df['average_temp']**2
df['average_temp_cube']    = df['average_temp']**3

#df['MA_temp_avg']         = df['average_temp'].rolling(window=24).mean()

df['month0_temp_avg']  = df['average_temp']* df['month_0']
df['month1_temp_avg']  = df['average_temp']* df['month_1']
df['month2_temp_avg']  = df['average_temp']* df['month_2']
df['month3_temp_avg']  = df['average_temp']* df['month_3']
df['month4_temp_avg']  = df['average_temp']* df['month_4']
df['month5_temp_avg']  = df['average_temp']* df['month_5']
df['month6_temp_avg']  = df['average_temp']* df['month_6']
df['month7_temp_avg']  = df['average_temp']* df['month_7']
df['month8_temp_avg']  = df['average_temp']* df['month_8']
df['month9_temp_avg']  = df['average_temp']* df['month_9']
df['month10_temp_avg'] = df['average_temp']* df['month_10']
df['month11_temp_avg'] = df['average_temp']* df['month_11']

df['month0_temp_avg_sqrd']  = df['average_temp_squared']* df['month_0']
df['month1_temp_avg_sqrd']  = df['average_temp_squared']* df['month_1']
df['month2_temp_avg_sqrd']  = df['average_temp_squared']* df['month_2']
df['month3_temp_avg_sqrd']  = df['average_temp_squared']* df['month_3']
df['month4_temp_avg_sqrd']  = df['average_temp_squared']* df['month_4']
df['month5_temp_avg_sqrd']  = df['average_temp_squared']* df['month_5']
df['month6_temp_avg_sqrd']  = df['average_temp_squared']* df['month_6']
df['month7_temp_avg_sqrd']  = df['average_temp_squared']* df['month_7']
df['month8_temp_avg_sqrd']  = df['average_temp_squared']* df['month_8']
df['month9_temp_avg_sqrd']  = df['average_temp_squared']* df['month_9']
df['month10_temp_avg_sqrd'] = df['average_temp_squared']* df['month_10']
df['month11_temp_avg_sqrd'] = df['average_temp_squared']* df['month_11']

df['month0_temp_avg_cubed'] = df['average_temp_cube']*df['month_0']
df['month1_temp_avg_cubed'] = df['average_temp_cube']*df['month_1']
df['month2_temp_avg_cubed'] = df['average_temp_cube']*df['month_2']
df['month3_temp_avg_cubed'] = df['average_temp_cube']*df['month_3']
df['month4_temp_avg_cubed'] = df['average_temp_cube']*df['month_4']
df['month5_temp_avg_cubed'] = df['average_temp_cube']*df['month_5']
df['month6_temp_avg_cubed'] = df['average_temp_cube']*df['month_6']
df['month7_temp_avg_cubed'] = df['average_temp_cube']*df['month_7']
df['month8_temp_avg_cubed'] = df['average_temp_cube']*df['month_8']
df['month9_temp_avg_cubed'] = df['average_temp_cube']*df['month_9']
df['month10_temp_avg_cubed'] = df['average_temp_cube']*df['month_10']
df['month11_temp_avg_cubed'] = df['average_temp_cube']*df['month_11']


df['h0_temp_avg_sqrd']  = df['average_temp_squared']*df['hour_0']
df['h1_temp_avg_sqrd']  = df['average_temp_squared']*df['hour_1']
df['h2_temp_avg_sqrd']  = df['average_temp_squared']*df['hour_2']
df['h3_temp_avg_sqrd']  = df['average_temp_squared']*df['hour_3']
df['h4_temp_avg_sqrd']  = df['average_temp_squared']*df['hour_4']
df['h5_temp_avg_sqrd']  = df['average_temp_squared']*df['hour_5']
df['h6_temp_avg_sqrd']  = df['average_temp_squared']*df['hour_6']
df['h7_temp_avg_sqrd']  = df['average_temp_squared']*df['hour_7']
df['h8_temp_avg_sqrd']  = df['average_temp_squared']*df['hour_8']
df['h9_temp_avg_sqrd']  = df['average_temp_squared']*df['hour_9']
df['h10_temp_avg_sqrd'] = df['average_temp_squared']*df['hour_10']
df['h11_temp_avg_sqrd'] = df['average_temp_squared']*df['hour_11']
df['h12_temp_avg_sqrd'] = df['average_temp_squared']*df['hour_12']
df['h13_temp_avg_sqrd'] = df['average_temp_squared']*df['hour_13']
df['h14_temp_avg_sqrd'] = df['average_temp_squared']*df['hour_14']
df['h15_temp_avg_sqrd'] = df['average_temp_squared']*df['hour_15']
df['h16_temp_avg_sqrd'] = df['average_temp_squared']*df['hour_16']
df['h17_temp_avg_sqrd'] = df['average_temp_squared']*df['hour_17']
df['h18_temp_avg_sqrd'] = df['average_temp_squared']*df['hour_18']
df['h19_temp_avg_sqrd'] = df['average_temp_squared']*df['hour_19']
df['h20_temp_avg_sqrd'] = df['average_temp_squared']*df['hour_20']
df['h21_temp_avg_sqrd'] = df['average_temp_squared']*df['hour_21']
df['h22_temp_avg_sqrd'] = df['average_temp_squared']*df['hour_22']
df['h23_temp_avg_sqrd'] = df['average_temp_squared']*df['hour_23']

df['h0_temp_avg_cbd'] = df['average_temp_cube']*df['hour_0']
df['h1_temp_avg_cbd'] = df['average_temp_cube']*df['hour_1']
df['h2_temp_avg_cbd'] = df['average_temp_cube']*df['hour_2']
df['h3_temp_avg_cbd'] = df['average_temp_cube']*df['hour_3']
df['h4_temp_avg_cbd'] = df['average_temp_cube']*df['hour_4']
df['h5_temp_avg_cbd'] = df['average_temp_cube']*df['hour_5']
df['h6_temp_avg_cbd'] = df['average_temp_cube']*df['hour_6']
df['h7_temp_avg_cbd'] = df['average_temp_cube']*df['hour_7']
df['h8_temp_avg_cbd'] = df['average_temp_cube']*df['hour_8']
df['h9_temp_avg_cbd'] = df['average_temp_cube']*df['hour_9']
df['h10_temp_avg_cbd'] = df['average_temp_cube']*df['hour_10']
df['h11_temp_avg_cbd'] = df['average_temp_cube']*df['hour_11']
df['h12_temp_avg_cbd'] = df['average_temp_cube']*df['hour_12']
df['h13_temp_avg_cbd'] = df['average_temp_cube']*df['hour_13']
df['h14_temp_avg_cbd'] = df['average_temp_cube']*df['hour_14']
df['h15_temp_avg_cbd'] = df['average_temp_cube']*df['hour_15']
df['h16_temp_avg_cbd'] = df['average_temp_cube']*df['hour_16']
df['h17_temp_avg_cbd'] = df['average_temp_cube']*df['hour_17']
df['h18_temp_avg_cbd'] = df['average_temp_cube']*df['hour_18']
df['h19_temp_avg_cbd'] = df['average_temp_cube']*df['hour_19']
df['h20_temp_avg_cbd'] = df['average_temp_cube']*df['hour_20']
df['h21_temp_avg_cbd'] = df['average_temp_cube']*df['hour_21']
df['h22_temp_avg_cbd'] = df['average_temp_cube']*df['hour_22']
df['h23_temp_avg_cbd'] = df['average_temp_cube']*df['hour_23']




df['month0_temp_calgary'] = df['temp_calgary']*df['month_0']
df['month1_temp_calgary'] = df['temp_calgary']*df['month_1']
df['month2_temp_calgary'] = df['temp_calgary']*df['month_2']
df['month3_temp_calgary'] = df['temp_calgary']*df['month_3']
df['month4_temp_calgary'] = df['temp_calgary']*df['month_4']
df['month5_temp_calgary'] = df['temp_calgary']*df['month_5']
df['month6_temp_calgary'] = df['temp_calgary']*df['month_6']
df['month7_temp_calgary'] = df['temp_calgary']*df['month_7']
df['month8_temp_calgary'] = df['temp_calgary']*df['month_8']
df['month9_temp_calgary'] = df['temp_calgary']*df['month_9']
df['month10_temp_calgary'] = df['temp_calgary']*df['month_10']
df['month11_temp_calgary'] = df['temp_calgary']*df['month_11']


df['month0_temp_calgary_squared'] = df['temp_calgary_squared']*df['month_0']
df['month1_temp_calgary_squared'] = df['temp_calgary_squared']*df['month_1']
df['month2_temp_calgary_squared'] = df['temp_calgary_squared']*df['month_2']
df['month3_temp_calgary_squared'] = df['temp_calgary_squared']*df['month_3']
df['month4_temp_calgary_squared'] = df['temp_calgary_squared']*df['month_4']
df['month5_temp_calgary_squared'] = df['temp_calgary_squared']*df['month_5']
df['month6_temp_calgary_squared'] = df['temp_calgary_squared']*df['month_6']
df['month7_temp_calgary_squared'] = df['temp_calgary_squared']*df['month_7']
df['month8_temp_calgary_squared'] = df['temp_calgary_squared']*df['month_8']
df['month9_temp_calgary_squared'] = df['temp_calgary_squared']*df['month_9']
df['month10_temp_calgary_squared'] = df['temp_calgary_squared']*df['month_10']
df['month11_temp_calgary_squared'] = df['temp_calgary_squared']*df['month_11']


df['month0_temp_calgary_cube'] = df['temp_calgary_cube']*df['month_0']
df['month1_temp_calgary_cube'] = df['temp_calgary_cube']*df['month_1']
df['month2_temp_calgary_cube'] = df['temp_calgary_cube']*df['month_2']
df['month3_temp_calgary_cube'] = df['temp_calgary_cube']*df['month_3']
df['month4_temp_calgary_cube'] = df['temp_calgary_cube']*df['month_4']
df['month5_temp_calgary_cube'] = df['temp_calgary_cube']*df['month_5']
df['month6_temp_calgary_cube'] = df['temp_calgary_cube']*df['month_6']
df['month7_temp_calgary_cube'] = df['temp_calgary_cube']*df['month_7']
df['month8_temp_calgary_cube'] = df['temp_calgary_cube']*df['month_8']
df['month9_temp_calgary_cube'] = df['temp_calgary_cube']*df['month_9']
df['month10_temp_calgary_cube'] = df['temp_calgary_cube']*df['month_10']
df['month11_temp_calgary_cube'] = df['temp_calgary_cube']*df['month_11']


df['hour0_temp_calgary_squared'] = df['temp_calgary_squared']*df['hour_0']
df['hour1_temp_calgary_squared'] = df['temp_calgary_squared']*df['hour_1']
df['hour2_temp_calgary_squared'] = df['temp_calgary_squared']*df['hour_2']
df['hour3_temp_calgary_squared'] = df['temp_calgary_squared']*df['hour_3']
df['hour4_temp_calgary_squared'] = df['temp_calgary_squared']*df['hour_4']
df['hour5_temp_calgary_squared'] = df['temp_calgary_squared']*df['hour_5']
df['hour6_temp_calgary_squared'] = df['temp_calgary_squared']*df['hour_6']
df['hour7_temp_calgary_squared'] = df['temp_calgary_squared']*df['hour_7']
df['hour8_temp_calgary_squared'] = df['temp_calgary_squared']*df['hour_8']
df['hour9_temp_calgary_squared'] = df['temp_calgary_squared']*df['hour_9']
df['hour10_temp_calgary_squared'] = df['temp_calgary_squared']*df['hour_10']
df['hour11_temp_calgary_squared'] = df['temp_calgary_squared']*df['hour_11']
df['hour12_temp_calgary_squared'] = df['temp_calgary_squared']*df['hour_12']
df['hour13_temp_calgary_squared'] = df['temp_calgary_squared']*df['hour_13']
df['hour14_temp_calgary_squared'] = df['temp_calgary_squared']*df['hour_14']
df['hour15_temp_calgary_squared'] = df['temp_calgary_squared']*df['hour_15']
df['hour16_temp_calgary_squared'] = df['temp_calgary_squared']*df['hour_16']
df['hour17_temp_calgary_squared'] = df['temp_calgary_squared']*df['hour_17']
df['hour18_temp_calgary_squared'] = df['temp_calgary_squared']*df['hour_18']
df['hour19_temp_calgary_squared'] = df['temp_calgary_squared']*df['hour_19']
df['hour20_temp_calgary_squared'] = df['temp_calgary_squared']*df['hour_20']
df['hour21_temp_calgary_squared'] = df['temp_calgary_squared']*df['hour_21']
df['hour22_temp_calgary_squared'] = df['temp_calgary_squared']*df['hour_22']
df['hour23_temp_calgary_squared'] = df['temp_calgary_squared']*df['hour_23']

df['hour0_temp_calgary_cube'] = df['temp_calgary_cube']*df['hour_0']
df['hour1_temp_calgary_cube'] = df['temp_calgary_cube']*df['hour_1']
df['hour2_temp_calgary_cube'] = df['temp_calgary_cube']*df['hour_2']
df['hour3_temp_calgary_cube'] = df['temp_calgary_cube']*df['hour_3']
df['hour4_temp_calgary_cube'] = df['temp_calgary_cube']*df['hour_4']
df['hour5_temp_calgary_cube'] = df['temp_calgary_cube']*df['hour_5']
df['hour6_temp_calgary_cube'] = df['temp_calgary_cube']*df['hour_6']
df['hour7_temp_calgary_cube'] = df['temp_calgary_cube']*df['hour_7']
df['hour8_temp_calgary_cube'] = df['temp_calgary_cube']*df['hour_8']
df['hour9_temp_calgary_cube'] = df['temp_calgary_cube']*df['hour_9']
df['hour10_temp_calgary_cube'] = df['temp_calgary_cube']*df['hour_10']
df['hour11_temp_calgary_cube'] = df['temp_calgary_cube']*df['hour_11']
df['hour12_temp_calgary_cube'] = df['temp_calgary_cube']*df['hour_12']
df['hour13_temp_calgary_cube'] = df['temp_calgary_cube']*df['hour_13']
df['hour14_temp_calgary_cube'] = df['temp_calgary_cube']*df['hour_14']
df['hour15_temp_calgary_cube'] = df['temp_calgary_cube']*df['hour_15']
df['hour16_temp_calgary_cube'] = df['temp_calgary_cube']*df['hour_16']
df['hour17_temp_calgary_cube'] = df['temp_calgary_cube']*df['hour_17']
df['hour18_temp_calgary_cube'] = df['temp_calgary_cube']*df['hour_18']
df['hour19_temp_calgary_cube'] = df['temp_calgary_cube']*df['hour_19']
df['hour20_temp_calgary_cube'] = df['temp_calgary_cube']*df['hour_20']
df['hour21_temp_calgary_cube'] = df['temp_calgary_cube']*df['hour_21']
df['hour22_temp_calgary_cube'] = df['temp_calgary_cube']*df['hour_22']
df['hour23_temp_calgary_cube'] = df['temp_calgary_cube']*df['hour_23']



df['month0_temp_edmonton'] = df['temp_edmonton']*df['month_0']
df['month1_temp_edmonton'] = df['temp_edmonton']*df['month_1']
df['month2_temp_edmonton'] = df['temp_edmonton']*df['month_2']
df['month3_temp_edmonton'] = df['temp_edmonton']*df['month_3']
df['month4_temp_edmonton'] = df['temp_edmonton']*df['month_4']
df['month5_temp_edmonton'] = df['temp_edmonton']*df['month_5']
df['month6_temp_edmonton'] = df['temp_edmonton']*df['month_6']
df['month7_temp_edmonton'] = df['temp_edmonton']*df['month_7']
df['month8_temp_edmonton'] = df['temp_edmonton']*df['month_8']
df['month9_temp_edmonton'] = df['temp_edmonton']*df['month_9']
df['month10_temp_edmonton'] = df['temp_edmonton']*df['month_10']
df['month11_temp_edmonton'] = df['temp_edmonton']*df['month_11']


df['month0_temp_edmonton_squared'] = df['temp_edmonton_squared']*df['month_0']
df['month1_temp_edmonton_squared'] = df['temp_edmonton_squared']*df['month_1']
df['month2_temp_edmonton_squared'] = df['temp_edmonton_squared']*df['month_2']
df['month3_temp_edmonton_squared'] =  df['temp_edmonton_squared']*df['month_3']
df['month4_temp_edmonton_squared'] = df['temp_edmonton_squared']*df['month_4']
df['month5_temp_edmonton_squared'] = df['temp_edmonton_squared']*df['month_5']
df['month6_temp_edmonton_squared'] = df['temp_edmonton_squared']*df['month_6']
df['month7_temp_edmonton_squared'] = df['temp_edmonton_squared']*df['month_7']
df['month8_temp_edmonton_squared'] = df['temp_edmonton_squared']*df['month_8']
df['month9_temp_edmonton_squared'] = df['temp_edmonton_squared']*df['month_9']
df['month10_temp_edmonton_squared'] = df['temp_edmonton_squared']*df['month_10']
df['month11_temp_edmonton_squared'] = df['temp_edmonton_squared']*df['month_11']


df['month0_temp_ftmcmry'] = df['temp_ftmcmry']*df['month_0']
df['month1_temp_ftmcmry'] = df['temp_ftmcmry']*df['month_1']
df['month2_temp_ftmcmry'] = df['temp_ftmcmry']*df['month_2']
df['month3_temp_ftmcmry'] = df['temp_ftmcmry']*df['month_3']
df['month4_temp_ftmcmry'] = df['temp_ftmcmry']*df['month_4']
df['month5_temp_ftmcmry'] = df['temp_ftmcmry']*df['month_5']
df['month6_temp_ftmcmry'] = df['temp_ftmcmry']*df['month_6']
df['month7_temp_ftmcmry'] = df['temp_ftmcmry']*df['month_7']
df['month8_temp_ftmcmry'] = df['temp_ftmcmry']*df['month_8']
df['month9_temp_ftmcmry'] = df['temp_ftmcmry']*df['month_9']
df['month10_temp_ftmcmry'] = df['temp_ftmcmry']*df['month_10']
df['month11_temp_ftmcmry'] = df['temp_ftmcmry']*df['month_11']


df['month0_temp_lthbrg'] = df['temp_lthbrg']*df['month_0']
df['month1_temp_lthbrg'] = df['temp_lthbrg']*df['month_1']
df['month2_temp_lthbrg'] = df['temp_lthbrg']*df['month_2']
df['month3_temp_lthbrg'] = df['temp_lthbrg']*df['month_3']
df['month4_temp_lthbrg'] = df['temp_lthbrg']*df['month_4']
df['month5_temp_lthbrg'] = df['temp_lthbrg']*df['month_5']
df['month6_temp_lthbrg'] = df['temp_lthbrg']*df['month_6']
df['month7_temp_lthbrg'] = df['temp_lthbrg']*df['month_7']
df['month8_temp_lthbrg'] = df['temp_lthbrg']*df['month_8']
df['month9_temp_lthbrg'] = df['temp_lthbrg']*df['month_9']
df['month10_temp_lthbrg'] = df['temp_lthbrg']*df['month_10']
df['month11_temp_lthbrg'] = df['temp_lthbrg']*df['month_11']


# In[17]:


#df.shape


# In[20]:


#for i in range(0, len(df.dtypes)):
#    print(i, "--->", df.columns[i], "--->", df.dtypes[i])


# In[24]:

'''
features= [    'hour_of_day' , 'off_peak' , 'on_peak'  , 'day',
       'sin.day', 'cos.day'  , 'sin.hour' , 'cos.hour' , 'weekend' , 'monday'   ,
       'tuesday', 'wednesday', 'thursday' , 'friday'   , 'saturday', 'sunday'   ,
       'month_0', 'month_1'  , 'month_2'  , 'month_3'  , 'month_4' , 'month_5'  ,
       'month_6', 'month_7'  , 'month_8'  , 'month_9'  , 'month_10', 'month_11' ,
       'hour_0' , 'hour_1'   , 'hour_2'   , 'hour_3'   , 'hour_4'  , 'hour_5'   , 
       'hour_6' , 'hour_7'   , 'hour_8'   , 'hour_9'   , 'hour_10' , 'hour_11'  , 
       'hour_12', 'hour_13'  , 'hour_14'  , 'hour_15'  , 'hour_16' , 'hour_17'  , 
       'hour_18', 'hour_19'  , 'hour_20'  , 'hour_21'  , 'hour_22' , 'hour_23'  , 
           
       #'year'                , 
       
       'sunlight_avaialbility' , 'AIL_previous_hour'   , 
       'AIL_24h_lagged'      , 'AIL_oneweek_lagged'    , 'holiday' , 
       'monday_holiday'      , 'tuesday_holiday'       , 'wednesday_holiday', 
       'thursday_holiday'    , 'friday_holiday'        , 'weekend_holiday'  , 
       
       'month0_mondayholiday' , 'month1_mondayholiday',
       'month2_mondayholiday' , 'month3_mondayholiday'   , 'month4_mondayholiday',
       'month5_mondayholiday' , 'month6_mondayholiday'   , 'month7_mondayholiday',
       'month8_mondayholiday' , 'month9_mondayholiday'   , 'month10_mondayholiday',
       'month11_mondayholiday', 'month0_fridayholiday'   , 'month1_fridayholiday',
       'month2_fridayholiday' , 'month3_fridayholiday'   , 'month4_fridayholiday',
       'month5_fridayholiday' , 'month6_fridayholiday'   , 'month7_fridayholiday',
       'month8_fridayholiday' , 'month9_fridayholiday'   , 'month10_fridayholiday',
       'month11_fridayholiday', 
       
       'sunlight_mondayholiday' ,'sunlight_tuesdayholiday',  
       
       'temp_calgary', 'wind_calgary', 'temp_edmonton', 'wind_edmonton', 'temp_ftmcmry',   
       'wind_ftmcmry', 'temp_lthbrg' , 'wind_lthbrg'  , 'temp_mdcnht'  , 'wind_mdcnht', 
       'temp_rddr'   , 'wind_rddr'   , 'temp_slvlk'   , 'wind_slvlk'   , 
        
       'temp_calgary_squared', 'temp_edmonton_squared' , 'temp_ftmcmry_squared', 'temp_lthbrg_squared', 
       'temp_mdcnht_squared' , 'temp_rddr_squared'     , 'temp_slvlk_squared'  , 'temp_calgary_cube'  , 
       'temp_edmonton_cube'  , 'temp_ftmcmry_cube'     , 'temp_lthbrg_cube'    , 'temp_mdcnht_cube'   , 
       'temp_rddr_cube'      , 'temp_slvlk_cube'       , 
       
       'wind_chill_index_calgary', 'wind_chill_index_edmonton', 'wind_chill_index_ftmcmry',
       'wind_chill_index_lthbrg' , 'wind_chill_index_mdcnht'  , 'wind_chill_index_rddr'   , 
       'wind_chill_index_slvlk', 
       
       #'MA_tempcalgary' , 'MA_tempedmonton', 'MA_tempftmcmry'      , 'MA_tempmdcnht'    , 'MA_temprddr', 
       #'MA_tempslvlk'   , 
        'average_temp'   , 'average_temp_squared', 'average_temp_cube', 
        #'MA_temp_avg', 
           
       'month0_temp_avg', 'month1_temp_avg', 'month2_temp_avg' , 'month3_temp_avg',
       'month4_temp_avg', 'month5_temp_avg', 'month6_temp_avg' , 'month7_temp_avg', 
       'month8_temp_avg', 'month9_temp_avg', 'month10_temp_avg', 'month11_temp_avg',
           
       'month0_temp_avg_sqrd' , 'month1_temp_avg_sqrd' , 'month2_temp_avg_sqrd',
       'month3_temp_avg_sqrd' , 'month4_temp_avg_sqrd' , 'month5_temp_avg_sqrd',
       'month6_temp_avg_sqrd' , 'month7_temp_avg_sqrd' , 'month8_temp_avg_sqrd',
       'month9_temp_avg_sqrd' , 'month10_temp_avg_sqrd','month11_temp_avg_sqrd', 
           
       'month0_temp_avg_cubed', 'month1_temp_avg_cubed' , 'month2_temp_avg_cubed',
       'month3_temp_avg_cubed', 'month4_temp_avg_cubed' , 'month5_temp_avg_cubed', 
       'month6_temp_avg_cubed', 'month7_temp_avg_cubed' , 'month8_temp_avg_cubed',
       'month9_temp_avg_cubed', 'month10_temp_avg_cubed','month11_temp_avg_cubed', 
           
       'h0_temp_avg_sqrd', 'h1_temp_avg_sqrd', 'h2_temp_avg_sqrd',    'h3_temp_avg_sqrd'  , 
       'h4_temp_avg_sqrd', 'h5_temp_avg_sqrd', 'h6_temp_avg_sqrd',    'h7_temp_avg_sqrd'  ,
       'h8_temp_avg_sqrd', 'h9_temp_avg_sqrd', 'h10_temp_avg_sqrd',   'h11_temp_avg_sqrd', 
       'h12_temp_avg_sqrd', 'h13_temp_avg_sqrd', 'h14_temp_avg_sqrd', 'h15_temp_avg_sqrd', 
       'h16_temp_avg_sqrd', 'h17_temp_avg_sqrd', 'h18_temp_avg_sqrd', 'h19_temp_avg_sqrd',
       'h20_temp_avg_sqrd', 'h21_temp_avg_sqrd', 'h22_temp_avg_sqrd', 'h23_temp_avg_sqrd', 
           
       'h0_temp_avg_cbd' , 'h1_temp_avg_cbd' , 'h2_temp_avg_cbd' ,  'h3_temp_avg_cbd' , 
       'h4_temp_avg_cbd' , 'h5_temp_avg_cbd' , 'h6_temp_avg_cbd' , 'h7_temp_avg_cbd',
       'h8_temp_avg_cbd' , 'h9_temp_avg_cbd' , 'h10_temp_avg_cbd', 'h11_temp_avg_cbd', 
       'h12_temp_avg_cbd', 'h13_temp_avg_cbd', 'h14_temp_avg_cbd', 'h15_temp_avg_cbd', 
       'h16_temp_avg_cbd', 'h17_temp_avg_cbd', 'h18_temp_avg_cbd', 'h19_temp_avg_cbd',
       'h20_temp_avg_cbd', 'h21_temp_avg_cbd', 'h22_temp_avg_cbd', 'h23_temp_avg_cbd', 
       
       'month0_temp_calgary', 'month1_temp_calgary' , 'month2_temp_calgary', 
       'month3_temp_calgary', 'month4_temp_calgary' , 'month5_temp_calgary', 
       'month6_temp_calgary', 'month7_temp_calgary' , 'month8_temp_calgary', 
       'month9_temp_calgary', 'month10_temp_calgary','month11_temp_calgary', 
           
       'month0_temp_calgary_squared','month1_temp_calgary_squared'  , 'month2_temp_calgary_squared',
       'month3_temp_calgary_squared', 'month4_temp_calgary_squared' , 'month5_temp_calgary_squared', 
       'month6_temp_calgary_squared', 'month7_temp_calgary_squared' , 'month8_temp_calgary_squared',
       'month9_temp_calgary_squared', 'month10_temp_calgary_squared','month11_temp_calgary_squared', 
       
       'month0_temp_calgary_cube' , 'month1_temp_calgary_cube' , 'month2_temp_calgary_cube', 
       'month3_temp_calgary_cube' , 'month4_temp_calgary_cube' , 'month5_temp_calgary_cube', 
       'month6_temp_calgary_cube' , 'month7_temp_calgary_cube' , 'month8_temp_calgary_cube', 
       'month9_temp_calgary_cube' ,'month10_temp_calgary_cube' , 'month11_temp_calgary_cube',
           
       'hour0_temp_calgary_squared'  , 'hour1_temp_calgary_squared',
       'hour2_temp_calgary_squared'  , 'hour3_temp_calgary_squared',
       'hour4_temp_calgary_squared'  , 'hour5_temp_calgary_squared',
       'hour6_temp_calgary_squared'  , 'hour7_temp_calgary_squared',
       'hour8_temp_calgary_squared'  , 'hour9_temp_calgary_squared',
       'hour10_temp_calgary_squared' , 'hour11_temp_calgary_squared',
       'hour12_temp_calgary_squared' , 'hour13_temp_calgary_squared',
       'hour14_temp_calgary_squared' , 'hour15_temp_calgary_squared',
       'hour16_temp_calgary_squared' , 'hour17_temp_calgary_squared',
       'hour18_temp_calgary_squared' , 'hour19_temp_calgary_squared',
       'hour20_temp_calgary_squared' , 'hour21_temp_calgary_squared',
       'hour22_temp_calgary_squared' , 'hour23_temp_calgary_squared',
           
       'hour0_temp_calgary_cube', 'hour1_temp_calgary_cube',
       'hour2_temp_calgary_cube', 'hour3_temp_calgary_cube',
       'hour4_temp_calgary_cube', 'hour5_temp_calgary_cube',
       'hour6_temp_calgary_cube', 'hour7_temp_calgary_cube',
       'hour8_temp_calgary_cube', 'hour9_temp_calgary_cube',
       'hour10_temp_calgary_cube', 'hour11_temp_calgary_cube',
       'hour12_temp_calgary_cube', 'hour13_temp_calgary_cube',
       'hour14_temp_calgary_cube', 'hour15_temp_calgary_cube',
       'hour16_temp_calgary_cube', 'hour17_temp_calgary_cube',
       'hour18_temp_calgary_cube', 'hour19_temp_calgary_cube',
       'hour20_temp_calgary_cube', 'hour21_temp_calgary_cube',
       'hour22_temp_calgary_cube', 'hour23_temp_calgary_cube',
       
       'month0_temp_edmonton', 'month1_temp_edmonton' , 'month2_temp_edmonton' ,
       'month3_temp_edmonton', 'month4_temp_edmonton' , 'month5_temp_edmonton' ,
       'month6_temp_edmonton', 'month7_temp_edmonton' , 'month8_temp_edmonton' ,
       'month9_temp_edmonton', 'month10_temp_edmonton', 'month11_temp_edmonton', 
           
       'month0_temp_edmonton_squared', 'month1_temp_edmonton_squared'  , 'month2_temp_edmonton_squared', 
       'month3_temp_edmonton_squared', 'month4_temp_edmonton_squared'  , 'month5_temp_edmonton_squared', 
       'month6_temp_edmonton_squared', 'month7_temp_edmonton_squared'  , 'month8_temp_edmonton_squared',
       'month9_temp_edmonton_squared', 'month10_temp_edmonton_squared' , 'month11_temp_edmonton_squared', 
           
       'month0_temp_ftmcmry' , 'month1_temp_ftmcmry' , 'month2_temp_ftmcmry' , 'month3_temp_ftmcmry',
       'month4_temp_ftmcmry' , 'month5_temp_ftmcmry' , 'month6_temp_ftmcmry' , 'month7_temp_ftmcmry' ,
       'month8_temp_ftmcmry' , 'month9_temp_ftmcmry' , 'month10_temp_ftmcmry', 'month11_temp_ftmcmry', 
           
       'month0_temp_lthbrg'  , 'month1_temp_lthbrg'  , 'month2_temp_lthbrg'  , 'month3_temp_lthbrg',
       'month4_temp_lthbrg'  , 'month5_temp_lthbrg'  , 'month6_temp_lthbrg'  , 'month7_temp_lthbrg'  ,
       'month8_temp_lthbrg'  , 'month9_temp_lthbrg'  , 'month10_temp_lthbrg' , 'month11_temp_lthbrg'   
        ]
'''
# In[17]:


#split_date = '2019-01-01 00:00:00'

#df_train = df.loc[df.Datetime < split_date].copy()
#df_test  = df.loc[df.Datetime >= split_date].copy()


df  = df.interpolate(method='nearest').ffill().bfill()

open_file   = open("feature_v2.pkl", "rb")
loaded_list = pickle.load(open_file)
open_file.close()
y_train = pd.DataFrame(df, columns = ['AIL'])
y_train = y_train.values
X_train = pd.DataFrame(df, columns = loaded_list)
X_train = X_train.values

#model with optimized hyperparameter
model   = XGBRegressor(colsample_bylevel=1,
                   colsample_bynode=1, 
                   colsample_bytree=0.7936909313888836, 
                   gamma=0.7977155727005966, 
                   importance_type='gain', 
                   learning_rate=0.1, 
                   max_delta_step=0,
                   max_depth=11, 
                   min_child_weight=3.0, 
                   missing=None, 
                   n_estimators=100,
                   n_jobs=1, 
                   nthread=None, 
                   objective='reg:squarederror',
                   random_state=0, 
                   reg_alpha=1, 
                   reg_lambda=0.8521680716712875, 
                   scale_pos_weight=1,
                   seed=None, 
                   silent=None, 
                   subsample=1, verbosity=1)

model.fit(X_train,y_train)

# save model to file
model_name = 'model_generated_on_{}_{}_{}.joblib.dat'.format(datetime.now().day, datetime.now().month, datetime.now().year)

joblib.dump(model, filename = model_name)

#df.to_csv('training_data_generated_on_{}_{}_{}.csv'.format(datetime.now().day, datetime.now().month, datetime.now().year)
