# The following libraries are always installed: JayDeBeApi, Pillow,h5py, ipykernel ,jupyter, matplotlib, numpy, pandas, scipy, sklearn
# The following libraries are always imported (edit imports and remove comment marks to change aliases):
## import sys
## import pandas as pd
## import numpy as np
## from pandas import DataFrame
# The results from the query above are saved to "data"

import requests, json, pandas as pd, datetime

# ------Configurations------
headers = {'Accept': 'application/json',
           'Authorization': 'Bearer <YOUR TOKEN>'}
# ---dataframe---#
dfadmins = pd.DataFrame(columns=['InsertTimeRecordUTC', 'id', 'email', 'full_name', 'away_mode_enabled',
                                 'auto_reassign_new_conv_to_default_inbox', 'open_count', 'closed_count'])


# ------Fetches------
def fetch_count(fetch_type, user):
    r = requests.get("https://api.intercom.io/counts?type=conversation&count=admin", headers=headers).text
    json_data = json.loads(r.strip())
    for i in json_data['conversation']['admin']:
        if i['id'] == user:
            return i[fetch_type]


# --------------------------------Get all admin objects
today = datetime.date.today().strftime('%Y-%m-%d')
r = requests.get("https://api.intercom.io/admins/", headers=headers).text
json_data = json.loads(r.strip())
for i in json_data['admins']:
    dfadmins = dfadmins.append({'InsertTimeRecordUTC': today,
                                'id': i['id'],
                                'email': i['email'],
                                'full_name': i['name'],
                                'away_mode_enabled': str(i['away_mode_enabled']),
                                'auto_reassign_new_conv_to_default_inbox': str(i['away_mode_reassign']),
                                'open_count': fetch_count('open', i['id']),
                                'closed_count': fetch_count('closed', i['id']),
                                }, ignore_index=True)

coolaResult = dfadmins