import requests
from datetime import datetime
import base64
import hashlib
import json
import uuid
import pytz
import pandas as pd

# --------------------------------Configurations
key = 'your key'
secret = 'your secret'

tz = pytz.timezone('Australia/Sydney')
Australia_now = datetime.now(tz)
url = "https://api.emarsys.net/api/v2/"
segment_id = str(datetime.now(tz).strftime("%Y-%m-%d-%H-%M-%S"))


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

dfadmins= pd.DataFrame(columns=["contact_list_id",
                                "contact_list_name",
                                "contact_uid",
                                "first_name",
                                "last_name",
                                "email"])

headers = generate_headers(key, secret, tz)
get_list_req = requests.get(url + 'contactlist', headers=headers)
list_req_data = json.loads(get_list_req.content)

if 'data' in list_req_data:
    for contact_list in list_req_data['data']:

        #   get contact details
        get_contact_req = requests.get(url + "contactlist/"+contact_list['id']+"/contacts/data?fields=1,2,3", headers=headers)
        list_contacts_data = json.loads(get_contact_req.content)
        if 'data' in list_contacts_data:
            for contact_data_id in list_contacts_data['data']:

                #   contact data
                contact = list_contacts_data['data'][contact_data_id]['fields']
                list_id = contact_list['id']
                list_name = contact_list['name']
                first_name = contact['1']
                last_name = contact['2']
                email = contact['3']
                uid = contact['uid']

                dfadmins = dfadmins.append({"contact_list_id": list_id,
                                            "contact_list_name": list_name,
                                            "contact_uid": uid,
                                            "first_name": first_name,
                                            "last_name": last_name,
                                            "email":email}, ignore_index=True)

coolaResult = dfadmins