import requests
import json
import datetime
import pandas as pd

#---twitch---#
channel_id_t = '9999999'
client_id = 'client_id'

#---dataframe---#
df= pd.DataFrame(columns=['InsertTimeRecordUTC','social_platform','followers_count'])

today = datetime.date.today()

#---twitch---#
headers = {'Accept': 'application/vnd.twitchtv.v5+json', 'Client-ID':client_id}
r = requests.get("https://api.twitch.tv/kraken/channels/"+channel_id_t+"/follows",headers = headers).text

json_data = json.loads(r.strip())
df = df.append({'InsertTimeRecordUTC'   : today,
                'social_platform' : 'twitch' ,
                'followers_count': json_data['_total']},ignore_index=True)

coolaResult = df