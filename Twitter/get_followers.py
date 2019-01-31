import requests
import json
import datetime
import pandas as pd

#---twitter---#
user_name='username'

#---dataframe---#
df= pd.DataFrame(columns=['InsertTimeRecordUTC','social_platform','followers_count'])

today = datetime.date.today()

#---twitter---#
r = requests.get("https://cdn.syndication.twimg.com/widgets/followbutton/info.json?screen_names="+user_name).text
json_data = json.loads(r.strip())
df = df.append({'InsertTimeRecordUTC'   : today,
                'social_platform' : 'twitter' ,
                'followers_count': json_data[0]['followers_count']}, ignore_index=True)

coolaResult = df