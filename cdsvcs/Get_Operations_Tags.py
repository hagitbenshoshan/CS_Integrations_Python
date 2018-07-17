#import tags data from cdsvcs’s api

import requests
from datetime import timedelta
from datetime import date
from datetime import datetime
import json
import pandas as pd

today = date.today()
d = datetime(
    year=today.year,
    month=today.month,
    day=today.day,
)
todayts      = datetime.utcnow().date()

#create an empty dataframe to be imported to Cooladata
df= pd.DataFrame(columns=['Category','Subject','Mood','DateRegister','InsertedBy','insert_time','costa_rica_DateRegister','OrigDateRegisterts'])

history_loop='<Insert time range amount in days(as Integer)>'

for i in range(1,history_loop):
    print i
    yesterday_ts = datetime.utcnow().date() - timedelta(days=i)
    begining_of_yesterday= str(todayts) +' 00:00:00.000000'
    #print begining_of_yesterday
    xdt = str(yesterday_ts)
	#print url
    url = "http://aaa.bbb.us/GetService.asmx/GetData?date="+ xdt #Your URL here 
    data = requests.get(url).text
    data = data[9:-17]
    # convert 'str' to Json
    json_data= json.loads(data.strip())
    for dt in json_data:
        v_Category   = dt['Category']
        v_DateRegister = str(pd.to_datetime(int(dt['DateRegister'] )  , unit='ms'))[0:10]
        v_Subject = dt['Subject']
        v_Mood = dt['Mood']
        v_InsertedBy = dt['InsertedBy']
        v_insert_time = datetime.utcnow()
        v_costa_rica_DateRegister = pd.to_datetime(int(dt['DateRegister'] ) - 21600000, unit='ms')
        v_OrigDateRegisterts = pd.to_datetime(int(dt['DateRegister'] )  , unit='ms')
        df = df.append({'Category': v_Category,
                    'DateRegister': v_DateRegister,
                    'Subject': v_Subject,
                    'Mood': v_Mood,
                    'InsertedBy': v_InsertedBy,
                    'DateRegister': v_DateRegister,
                    'costa_rica_DateRegister': v_costa_rica_DateRegister,
                    'OrigDateRegisterts' :v_OrigDateRegisterts,
                    'insert_time': v_insert_time
                    }, ignore_index=True)
coolaResult = df
