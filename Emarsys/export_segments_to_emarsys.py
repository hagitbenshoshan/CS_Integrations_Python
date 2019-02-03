
"""
CQL: select * from logged_in_in_the_last_7_days

"""


import pandas as pd
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

url = "https://api.emarsys.net/api/v2/contactlist"
segment_id = str(datetime.now(tz).strftime("%Y-%m-%d-%H-%M-%S"))

payload = {
    "key_id": "3",
    "name": "Contact_List_" + segment_id,
}

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


def generate_payload(df, payload):
    payload["external_ids"] = df["user_alternative_id"].tolist()
    return json.dumps(payload)


# --------------------------------Main
# X-WSSE: UsernameToken Username="_your_key_", PasswordDigest="_digest_", Nonce="_nonce_", Created="2015-10-19T10:22:35Z"

iterator = 0
while (iterator <= data.shape[0]):
    payload = generate_payload(data[iterator:(iterator + 1000)], payload)
    print(payload)
    headers = generate_headers(key, secret, tz)
    r = requests.post(url, headers=headers, data=payload).text
    print(r)
    iterator += 1000