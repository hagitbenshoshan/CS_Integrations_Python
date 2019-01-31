import requests
import json
import datetime
import pandas as pd

#---instagram---#
uid='99999999999'

#---dataframe---#
df= pd.DataFrame(columns=['InsertTimeRecordUTC','social_platform','followers_count'])

today = datetime.date.today()

#---instagram---#
r = requests.get("https://graph.facebook.com/"+action+"&access_token="+token).text
json_data = json.loads(r.strip())
df = df.append({'InsertTimeRecordUTC'   : today,
                'social_platform' : 'instagram' ,
                'followers_count': json_data['instagram_accounts']['data'][0]['followed_by_count']},ignore_index=True)

coolaResult = df