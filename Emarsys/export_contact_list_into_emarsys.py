import requests
from datetime import datetime
# import oauth2client
import base64
import hashlib
import json
import uuid
import pytz


# --------------------------------Configurations
key = 'your_key'
secret = 'your_secret'

tz = pytz.timezone('Australia/Sydney')
Australia_now = datetime.now(tz)
contact_url = "https://api.emarsys.net/api/v2/contactlist"
i = datetime.now(tz).isoformat()


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


def make_request_to_cd(payload):
    project_id = "121204"
    user_token = "Token jLzGvpcDJBSjRkWd3PmjmsWlZda3Pih737917SPc"
    headers = {"authorization": user_token}
    url = "https://app.cooladata.com/api/v2/projects/" + project_id + "/cql"
    r = requests.post(url, data=payload, headers=headers).text
    data = json.loads(r)["table"]["rows"]
    emails_list = []
    emails_batch = []
    count = 1
    for i in data:
        if count % 9999 != 0:
            emails_batch.append(str(i['c'][0]['v']))
        else:
            emails_list.append(emails_batch[:])
            emails_batch = []
            count = 1
        count += 1

    if (len(data) % 9999 != 0):
        emails_list.append(emails_batch[:])
    return emails_list


# ====================main====================
for i in data.iterrows():
    segment = str(i[1])
    segment = segment.split("    ", 1)[1]
    segment = segment.split("\n", 1)[0]
    query = str("tq=select * from " + segment)
    contact_payload = {"key_id": "3"}
    emails = make_request_to_cd(query)
    count = 0
    for j in emails:
        headers = generate_headers(key, secret, tz)
        contact_payload["external_ids"] = j
        contact_payload["name"] = "Contact_List_" + str(count) + "_" + segment
        print(contact_payload)
        r = requests.post(contact_url, headers=headers, data=contact_payload).text
        print(r)
        count += 1

coolaResult = data