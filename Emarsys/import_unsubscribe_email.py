import pandas as pd
import requests
from datetime import datetime
from datetime import timedelta
import time
import base64
import hashlib
import json
import uuid
import pytz
import ciso8601
from io import StringIO

tz = pytz.timezone('Australia/Sydney')
Australia_now = datetime.now(tz)
#--------------------------------Configurations

key = 'your_key'
secret = 'your_secret'

segment_id=str(datetime.now(tz).strftime("%Y-%m-%d-%H-%M-%S"))

#Email Status codes: 1-In design, 2-Tested, 3-Launched, 4-Ready to launch, -3 Deactivated, -6 Aborted
url_email_campaigns = "https://api.emarsys.net/api/v2/email/?status=3"

url_query ="https://api.emarsys.net/api/v2/email/getresponses"

url_contact_data = "https://api.emarsys.net/api/v2/contact/getdata"

df= pd.DataFrame(columns=['event_time_ts',
                          'creation_time',
                          'event_name',
                          'email_campaign_id',
                          'email_name',
                          'from_email',
                          'from_name',
                          'subject',
                          'unsubscribe_option_available',
                          'user_id',
                          'user_email'
                            ])

i = datetime.now(tz).isoformat()
#--------------------------------End-Configurations
#--------------------------------Functions
def generate_headers(key, secret,tz):
    timestamp = datetime.now(tz).isoformat()
    nonce = uuid.uuid4().hex
    digest = '{}{}{}'.format(nonce, timestamp , secret)
    #print digest
    hashed_digest = bytes(hashlib.sha1(digest.encode()).hexdigest())
    encoded_hashed_digest = base64.b64encode(hashed_digest).decode("utf-8")

    username_token = 'UsernameToken Username="{}", PasswordDigest="{}", Nonce="{}", Created="{}"'.format(
            key, encoded_hashed_digest, nonce, timestamp)
    headers = {
        'Authorization': 'WSSE profile="UsernameToken"',
        'X-WSSE': username_token,
        'Accept-charset': 'utf-8',
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

    return headers

def generate_headers_csv(key, secret,tz):
    timestamp = datetime.now(tz).isoformat()
    nonce = uuid.uuid4().hex
    digest = '{}{}{}'.format(nonce, timestamp , secret)
    #print digest
    hashed_digest = bytes(hashlib.sha1(digest.encode()).hexdigest())
    encoded_hashed_digest = base64.b64encode(hashed_digest).decode("utf-8")

    username_token = 'UsernameToken Username="{}", PasswordDigest="{}", Nonce="{}", Created="{}"'.format(
            key, encoded_hashed_digest, nonce, timestamp)
    headers = {
        'Authorization': 'WSSE profile="UsernameToken"',
        'X-WSSE': username_token,
        'Accept-charset': 'utf-8',
        'Accept': 'text/csv',
        'Content-Type': 'text/csv'
    }

    return headers

def generate_payload_list(email_id):
  to_date = (datetime.today()-timedelta(1)).strftime('%Y-%m-%d %H:%M')
  from_date= (datetime.today()-timedelta(2)).strftime('%Y-%m-%d %H:%M')

  payload = {
   "distribution_method":"local",
	"sources": ["unsubscribe"],
   "email_id": email_id,
	"time_range": [from_date ,to_date],
   "contact_fields": [1,3],
   "analysis_fields": [1,5,8,14,15],
   "delimiter": ";",
   "add_field_names_header": 1,
   "language": "en"
   }
  return json.dumps(payload)

#query codes (request code,email_id)
def generate_rows (query_codes,df):
    dfnew= pd.DataFrame(columns=['event_time_ts','creation_time','event_name',
                          'email_campaign_id','email_name','from_email','from_name','subject','unsubscribe_option_available','user_id','user_email'])

    for i in query_codes:
        url_query_response = "https://api.emarsys.net/api/v2/export/"+str(i[0])+"/data/?offset=0"
        r = requests.get(url_query_response,headers=generate_headers(key, secret,tz)).text
        dfr = pd.read_csv(StringIO(r), sep=';')
        row=df[df["email_campaign_id"] == i[1]].head(1).reset_index(drop=True)
        for index, dfrow in dfr.iterrows():
            timeutc = (tz.normalize(tz.localize(datetime.strptime(dfrow['Time'], '%d-%m-%Y %H:%M:%S'))).astimezone(pytz.utc)).strftime('%Y-%m-%d %H:%M')
            dfnew = dfnew.append({"event_time_ts" : str(timeutc),
                            "creation_time"     : row.loc[0,'creation_time'],
                            "event_name"        : "unsubscribe",
                            "email_campaign_id" : str(row.loc[0,'email_campaign_id']),
                            "email_name"        : str(row.loc[0,'email_name']),
                            "from_email"        : str(row.loc[0,'from_email']),
                            "from_name"         : str(row.loc[0,'from_name']),
                            "subject"           : str(row.loc[0,'subject']),
                            "unsubscribe_option_available"  :str(row.loc[0,"unsubscribe_option_available"]),
                            "user_id"           : str(dfrow['user_id']),
                            "user_email"        : str(dfrow['Email'])
                            },ignore_index=True)
    return dfnew
#--------------------------------Main

#this request gets all email campaigns
r = requests.get(url_email_campaigns,headers=generate_headers(key, secret,tz)).text
json_data = json.loads(r.strip())

for i in json_data["data"]:
  if ciso8601.parse_datetime(i["created"]) > (datetime.today()-timedelta(30)):
    df = df.append({  'creation_time'                   : datetime.strptime(i["created"],'%Y-%m-%d %H:%M:%S'),
                      'email_campaign_id'               : i["id"],
                      'email_name'                      : i["name"],
                      'from_email'                      : i["fromemail"] ,
                      'from_name'                       : i["fromname"] ,
                      'subject'                         : i["subject"] ,
                      'unsubscribe_option_available'    : i["unsubscribe"]
                      },ignore_index=True)

query_codes = []

#df is the list of email campaigns
for i, row in df.iterrows():
  payload=generate_payload_list(row['email_campaign_id'])
  r = requests.post(url_query,headers=generate_headers(key, secret,tz),data=payload).text
  query_codes.append((json.loads(r)["data"]["id"],row['email_campaign_id']))
  time.sleep(10)

while True:
  try:
    dfnew=generate_rows(query_codes, df)
  except:
    time.sleep(420)
    dfnew=generate_rows(query_codes, df)
  else:
    break

coolaResult = dfnew
