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

# --------------------------------Configurations
key = 'your_key'
secret = 'your_secret'

tz = pytz.timezone('Australia/Sydney')
Australia_now = datetime.now(tz)
segment_id = str(datetime.now(tz).strftime("%Y-%m-%d-%H-%M-%S"))

queryId = ""

# Email Status codes: 1-In design, 2-Tested, 3-Launched, 4-Ready to launch, -3 Deactivated, -6 Aborted
url_email_campaigns = "https://api.emarsys.net/api/v2/email/?status=3"

url_query = "https://api.emarsys.net/api/v2/email/responses"

url_query_response = "https://api.emarsys.net/api/v2/email/" + queryId + "/responses"

url_contact_data = "https://api.emarsys.net/api/v2/contact/getdata"
# url_Query_Mail_Event = "https://api.emarsys.net/api/rtm/mail_events/unsubscribes]?from="+from_date+"&to="+to_date

# event_list=['received','opened','clicked']
event_list = ['clicked']

df = pd.DataFrame(columns=['event_time_ts', 'creation_time', 'event_name',
                           'email_campaign_id', 'email_name', 'from_email', 'from_name', 'subject',
                           'unsubscribe_option_available', 'user_id', 'user_email'
                           ])

dfnew = pd.DataFrame(columns=['event_time_ts', 'creation_time', 'event_name',
                              'email_campaign_id', 'email_name', 'from_email', 'from_name', 'subject',
                              'unsubscribe_option_available', 'user_id', 'user_email'
                              ])
i = datetime.now(tz).isoformat()


# --------------------------------End-Configurations
# --------------------------------Functions
def generate_headers(key, secret, tz):
    timestamp = datetime.now(tz).isoformat()
    nonce = uuid.uuid4().hex
    digest = '{}{}{}'.format(nonce, timestamp, secret)

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


def generate_payload_list(event_type, df):
    to_date = (datetime.today() - timedelta(1)).strftime('%Y-%m-%d %H:%M')
    from_date = (datetime.today() - timedelta(2)).strftime('%Y-%m-%d %H:%M')
    # Allowed Values: opened, not_opened, received, clicked, not_clicked, bounced, hard_bounced, soft_bounced, block_bounced

    payloads = []
    for i in df['email_campaign_id']:
        payload = {
            "type": event_type,
            "start_date": from_date,
            "end_date": to_date,
            # "campaign_id": 4594616,
            "campaign_id": i
        }
        payloads.append(json.dumps(payload))
    return payloads


def generate_rows(query_codes, event_type, df, dfnew):
    for query_and_campaign in query_codes:
        url_contact_data = "https://api.emarsys.net/api/v2/contact/getdata"
        url_query_response = "https://api.emarsys.net/api/v2/email/" + str(query_and_campaign[0]) + "/responses"

        # this request returns the query results. stays available for 2 hours only
        r = requests.get(url_query_response, headers=generate_headers(key, secret, tz)).text

        if (json.loads(r)["data"] == ""):
            continue
        contacts = json.loads(r)["data"]["contact_ids"]
        if contacts != [''] and contacts != [""]:
            for contact in contacts:
                contact_as_list = [contact]
                payload = {
                    "keyId": "id",
                    "keyValues": contact_as_list,
                    "fields": ["3"]
                }
                r = requests.post(url_contact_data, headers=generate_headers(key, secret, tz),
                                  data=json.dumps(payload)).text
                contact_email = json.loads(r)["data"]["result"][0]["3"]
                num = query_and_campaign[1]

                if not df[df["email_campaign_id"] == num].empty:
                    row = df[df["email_campaign_id"] == num].head(1).reset_index(drop=True)
                    dfnew = dfnew.append({"event_time_ts": datetime.now(),
                                          "creation_time": row.loc[0, 'creation_time'],
                                          "event_name": event_type,
                                          "email_campaign_id": str(row.loc[0, 'email_campaign_id']),
                                          "email_name": str(row.loc[0, 'email_name']),
                                          "from_email": str(row.loc[0, 'from_email']),
                                          "from_name": str(row.loc[0, 'from_name']),
                                          "subject": str(row.loc[0, 'subject']),
                                          "unsubscribe_option_available": str(
                                              row.loc[0, "unsubscribe_option_available"]),
                                          "user_id": str(contact),
                                          "user_email": str(contact_email)
                                          }, ignore_index=True)
    return dfnew


# --------------------------------Main

headers = generate_headers(key, secret, tz)

# this request gets all email campaigns
r = requests.get(url_email_campaigns, headers=generate_headers(key, secret, tz)).text
json_data = json.loads(r.strip())

for i in json_data["data"]:
    if ciso8601.parse_datetime(i["created"]) > (datetime.today() - timedelta(30)):
        df = df.append({'creation_time': datetime.strptime(i["created"], '%Y-%m-%d %H:%M:%S'),
                        'email_campaign_id': i["id"],
                        'email_name': i["name"],
                        'from_email': i["fromemail"],
                        'from_name': i["fromname"],
                        'subject': i["subject"],
                        'unsubscribe_option_available': i["unsubscribe"]
                        }, ignore_index=True)

query_codes = []
for i in event_list:
    payloads = generate_payload_list(i, df)
    for j in payloads:

        # this request sends a query
        r = requests.post(url_query, headers=generate_headers(key, secret, tz), data=j).text
        query_codes.append((json.loads(r)["data"]["id"], json.loads(j)["campaign_id"]))
        time.sleep(60)

    print(query_codes)
    try:
        dfnew = generate_rows(query_codes, i, df, dfnew)
    except:
        query_codes = []
    query_codes = []

print(dfnew)

coolaResult = dfnew