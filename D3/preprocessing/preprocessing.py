# -*- coding: utf-8 -*-
"""
Created on Fri Oct 18 16:17:58 2019

@author: caoa
"""
import pandas as pd

pd.options.display.max_rows =20
pd.options.display.max_columns = 20

df = pd.read_csv('GL2018.TXT', header=None, usecols=[0,3,6,9,10])
df.columns = ['date','away','home','aRuns','hRuns']

#%%
df['team'] = df.apply(lambda x: x['away'] if x['aRuns'] > x['hRuns'] else x['home'], axis=1)
data = df[['date','team']]
data.to_csv('daily_snapshot.csv', index=False)

#%% Find first day where all teams have won at least one game
data['date'] = pd.to_datetime(data['date'], format='%Y%m%d')
daterange = pd.date_range('2018-03-29','2018-10-01',freq='D')
for day in daterange:
    abc = data[data['date'] <= day]
    xyz = abc.team.value_counts()
    if xyz.shape[0] >= 30:
        print(day)
        break
