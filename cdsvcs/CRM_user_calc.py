# import player data from internal api

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
todayts = datetime.utcnow().date()

# create an empty dataframe to be imported to Cooladata
df = pd.DataFrame(
    columns=['username', 'SignUpDate', 'TZ_SignUpDate', 'OrigSignUpDatets', 'Skin', 'Email', 'insert_time'])

history_loop = '<Insert time range amount in days(as Integer)>'

for i in range(1, history_loop):
    print
    i
    yesterday_ts = datetime.utcnow().date() - timedelta(days=i)
    begining_of_yesterday = str(todayts) + ' 00:00:00.000000'

    xdt = str(yesterday_ts)

    url = "http://aaa.bbb.us/Users.asmx/GetUsers?date=" + xdt  # Insert your URL here 
    data = requests.get(url).text
    data = data[9:-17]
    # convert 'str' to Json
    json_data = json.loads(data.strip())
    for dt in json_data:
        v_username = dt['Username']
        v_SignUpDate = str(pd.to_datetime(int(dt['SignUpDate']), unit='ms'))[0:10]
        v_Skin = dt['Skin']
        v_Email = dt['Email']
        v_insert_time = datetime.utcnow()
        v_TZ_SignUpDate = pd.to_datetime(int(dt['SignUpDate']) - 21600000, unit='ms')
        v_OrigSignUpDatets = pd.to_datetime(int(dt['SignUpDate']), unit='ms')
        df = df.append({'username': v_username,
                        'SignUpDate': v_SignUpDate,
                        'TZ_SignUpDate': v_costa_rica_SignUpDate,
                        'OrigSignUpDatets': v_OrigSignUpDatets,
                        'Skin': v_Skin,
                        'Email': v_Email,
                        'insert_time': v_insert_time
                        }, ignore_index=True)
coolaResult = df

