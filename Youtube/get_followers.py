import requests
import json
import datetime
import pandas as pd

#---youtube---#
channel_id_y='UCUm4P0v0KYKFuGfxNTqKGHw'
api_key = 'AIzaSyAV0vfK8hXtlCPeFA0skR9vrOYDD5k1SzY'

#---dataframe---#
df= pd.DataFrame(columns=['InsertTimeRecordUTC','social_platform','followers_count'])

today = datetime.date.today()

#---youtube---#
r = requests.get("https://www.googleapis.com/youtube/v3/channels?part=statistics&id="+channel_id_y+"&key="+api_key).text
json_data = json.loads(r.strip())
df = df.append({'InsertTimeRecordUTC'   : today,
                'social_platform' : 'youtube' ,
                'followers_count': json_data['items'][0]['statistics']['subscriberCount']}, ignore_index=True)

coolaResult = df