# Brings data from Brandwatch’s api and counts the number of mentions.
import logging
import requests
from datetime import datetime, date, time, timedelta
import json
import pandas as pd

logger = logging.getLogger("bwapi")

USER_NAME = 'your_username@example.com'
PASSWORD = 'your_password'

address = 'https://newapi.brandwatch.com/oauth/token?username=%s' \
          '&grant_type=api-password&client_id=brandwatch-api-client' % USER_NAME

data = {'password': PASSWORD}

r = requests.post(address, data=data)
jsonToPython = json.loads(r.content)
my_access_token = jsonToPython['access_token']
token = my_access_token


url = 'https://newapi.brandwatch.com/projects/<Insert Project number here>'
headers = {'Authorization': 'bearer %s' % my_access_token}

resp = requests.get(url=url, headers=headers)

today = date.today()
d = datetime(
    year=today.year,
    month=today.month,
    day=today.day,
)
todayts = datetime.utcnow().date()
until_dt = todayts - timedelta(days=0)

# Create the recentInboundActivitySearchRequest passed into
begining_of_yesterday = str(todayts) + ' 00:00:00.000000'
midnight = datetime.combine(date.today(), time.min)
yesterday_midnight = midnight - timedelta(days=1)
fromdate = str(yesterday_midnight)[:10] + 'T00%3A00%3A00.000Z'
todate = str(midnight)[:10] + 'T00%3A00%3A00.000Z'

# Queries
url = 'https://newapi.brandwatch.com/projects/1998169013/data/mentions/count?endDate=' + todate + '&queryId%5B%5D=1998336099&startDate=' + fromdate

# print url
headers = {'Authorization': 'bearer %s' % my_access_token}  # ACR FACEBOOK
response = requests.get(url=url, headers=headers)

d = json.loads(response.text)

if response.ok:
    mentionsCount = str(d["mentionsCount"])
else:
    print("Could not get campaigns (status code={}, content='{}', headers={})".format(
        response.status_code, response.text, response.headers
    ))

# Get into DF
df = pd.DataFrame(columns=['date', 'mentionsCount'])
df = df.append({'date': str(yesterday_midnight),
                'mentionsCount': mentionsCount
                }, ignore_index=True)

coolaResult = df

