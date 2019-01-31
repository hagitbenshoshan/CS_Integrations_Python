# from apiclient.discovery import build
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import json, os, pandas as pd
from datetime import date, timedelta

# --------------------------------Configurations
ids = 'GOOGLE_ID'
start_date = (date.today() - timedelta(2)).strftime('%Y-%m-%d')
end_date = (date.today() - timedelta(1)).strftime('%Y-%m-%d')

metric_pageview = 'ga:pageviews'

dimension_week = 'ga:week'
dimension_location = 'ga:goalCompletionLocation'
dimension_prev_steps = 'ga:goalPreviousStep1,ga:goalPreviousStep2,ga:goalPreviousStep3'
dimension_landingpage = 'ga:landingPagePath'

# Define the auth scopes to request.
scope = 'https://www.googleapis.com/auth/analytics.readonly'
key_file_location = '/output/data.json'

# ---dataframe---#
df = pd.DataFrame(columns=['start_time_utc', 'Landing_page', 'Pageviews', 'affiliate_name'])


# --------------------------------End configurations

# --------------------------------Functions
def get_service(api_name, api_version, scopes, key_file_location):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        key_file_location, scopes=scopes)

    # Build the service object.
    service = build(api_name, api_version, credentials=credentials)

    return service


def create_json_file(key_file_location):
    json_data = {"Service account json"}

    with open(key_file_location, 'w') as outfile:
        json.dump(json_data, outfile)


def delete_json_file(key_file_location):
    if os.path.exists(key_file_location):
        os.remove(key_file_location)


def get_impr(service, ids, start_date, end_date, metrics, dimensions):
    goals = service.data().ga().get(
        ids=ids,
        start_date=start_date,
        end_date=end_date,
        metrics=metrics,
        dimensions=dimensions
    ).execute()
    return goals


def get_affiliate(url):
    if url.find('tm/') != -1 or url.find('-tm') != -1 or url.find('/tm') != -1:
        return 'TrafficMoose'

    if url.find('at/') != -1 or url.find('-at') != -1 or url.find('/at') != -1:
        return 'Adsterra'

    if url.find('wm/') != -1 or url.find('-wm') != -1 or url.find('/wm') != -1:
        return 'TwitchDigitalInternal'

    if url.find('ar/') != -1 or url.find('-ar') != -1 or url.find('/ar') != -1:
        return 'AdRight'

    if url.find('ha/') != -1 or url.find('-ha') != -1 or url.find('/ha') != -1:
        return 'HilltopAds'

    if url.find('as/') != -1 or url.find('-as') != -1 or url.find('/as') != -1:
        return 'AdSupply'

    if url.find('ah/') != -1 or url.find('-ah') != -1 or url.find('/ah') != -1:
        return 'AdHit'

    if url.find('ja/') != -1 or url.find('-ja') != -1 or url.find('/ja') != -1:
        return 'JuicyAds'

    return 'General'


# --------------------------------Main
create_json_file(key_file_location)
# Authenticate and construct service.
service = get_service(
    api_name='analytics',
    api_version='v3',
    scopes=[scope],
    key_file_location=key_file_location)

# get all goals matrics and dimensions, should have done that in a for loop using the amount of goals there are.
goals = get_impr(service, ids, start_date, end_date, metric_pageview, dimension_landingpage)
print(goals)

for i in goals['rows']:
    df = df.append({'start_time_utc': start_date,
                    'Landing_page': i[0],
                    'Pageviews': i[1],
                    'affiliate_name': get_affiliate(i[0])}, ignore_index=True)

delete_json_file(key_file_location)
coolaResult = df