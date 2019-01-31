# This code brings data from Bronto’s API to Cooladata

import json
import requests
import pandas as pd

url = "https://auth.bronto.com/oauth2/token/"

# In this part you need to apply your credentials
CLIENT_ID = "<enter your ID>"
CLIENT_SECRET = "<enter your secret>"

grant_type = 'client_credentials'
AUTHORIZE_URL = "https://auth.bronto.com/oauth2/authorize/"
ACCESS_TOKEN_URL = "https://auth.bronto.com/oauth2/token/"
REDIRECT_URI = 'https://auth.bronto.com/oauth2_redirect'

# Ask for an authorization code
requests.get('{}?response_type=code&client_id={}&redirect_uri={}'.format(AUTHORIZE_URL, CLIENT_ID, REDIRECT_URI))

# The user logs in, accepts your client authentication request
r = requests.post(
    ACCESS_TOKEN_URL,
    data={
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'redirect_uri': REDIRECT_URI
    }
)

body = json.loads(r.content)
token = body["access_token"]
expiresIn = body["expires_in"]
refresh_token = body["refresh_token"]

session = requests.Session()
session.headers["Authorization"] = "Bearer " + token

# The url last part is pointing to our integration (in this case “campaigns”)
url = "https://rest.bronto.com/campaigns/"

response = session.get(url)

if response.ok:
    campaigns = response.json()
else:
    print("Could not get campaigns (status code={}, content='{}', headers={})".format(
        response.status_code, response.text, response.headers
    ))

# Define result dataframe
df = pd.DataFrame(
    columns=['archived',
             'campaignTypeId',
             'name',
             'siteId',
             'modifiedDate',
             'createdDate',
             'id',
             'description'])

# load response data into dataframe
for campaign in campaigns:
    v_archived = ''
    v_campaignTypeId = ''
    v_name = ''
    v_siteId = ''
    v_modifiedDate = ''
    v_createdDate = ''
    v_id = ''
    v_description = ''
    v_createdDate = str(campaign["createdDate"])[:19].replace("T", " ")
    v_modifiedDate = str(campaign["modifiedDate"])[:19].replace("T", " ")
    v_name = campaign["name"].encode('utf-8').strip()
    v_siteId = campaign["siteId"]
    v_archived = str(campaign["archived"])
    v_campaignTypeId = campaign["campaignTypeId"]
    v_id = campaign["id"]
    v_description = campaign["description"]

    df = df.append({'archived': v_archived,
                    'createdDate': v_createdDate,
                    'modifiedDate': v_modifiedDate,
                    'name': v_name,
                    'siteId': v_siteId,
                    'campaignTypeId': v_campaignTypeId,
                    'id': v_id,
                    'description': v_description
                    }, ignore_index=True)

# assign the result to "coolaResult" variable
# which will be uploaded into CoolaData
coolaResult = df

