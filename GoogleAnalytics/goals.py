# from apiclient.discovery import build
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import json, os, pandas as pd
from datetime import date, timedelta

# --------------------------------Configurations
ids = 'GOOGLE_ID'
start_date = (date.today() - timedelta(2)).strftime('%Y-%m-%d')
end_date = (date.today() - timedelta(1)).strftime('%Y-%m-%d')

metric_start = 'ga:goal1Starts,ga:goal2Starts,ga:goal3Starts,ga:goalStartsAll'
metric_completion = 'ga:goal1Completions,ga:goal2Completions,ga:goal3Completions,ga:goalCompletionsAll'
metric_value = 'ga:goal1Value, ga:goal2Value,ga:goal3Value, ga:goalValueAll,'
metric_value_per_session = 'ga:goalValuePerSession'
metric_conv_rate = 'ga:goal1ConversionRate,ga:goal2ConversionRate,ga:goal3ConversionRate,ga:goalConversionRateAll'
metric_abandons = 'ga:goal1Abandons,ga:goal2Abandons,ga:goal3Abandons,ga:goalAbandonsAll'
metric_abandon_rate = 'ga:goal1AbandonRate,ga:goal2AbandonRate,ga:goal3AbandonRate,ga:goalAbandonRateAll'

dimension_week = 'ga:week'
dimension_location = 'ga:goalCompletionLocation'
dimension_prev_steps = 'ga:goalPreviousStep1,ga:goalPreviousStep2,ga:goalPreviousStep3'

# Define the auth scopes to request.
scope = 'https://www.googleapis.com/auth/analytics.readonly'
key_file_location = '/output/data.json'

# ---dataframe---#
df = pd.DataFrame(columns=['start_time_utc', 'Account_Creation_Step1_Completions',
                           'Account_Creation_Step2_Completions',
                           'Account_Creation_Step3_Completions', 'Goal_Completions',
                           'Conversion_Rate_1', 'Conversion_Rate_2', 'Conversion_Rate_3', 'Conversion_Rate_All'])


# --------------------------------End configurations

# --------------------------------Functions
def get_service(api_name, api_version, scopes, key_file_location):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        key_file_location, scopes=scopes)

    # Build the service object.
    service = build(api_name, api_version, credentials=credentials)

    return service


def create_json_file(key_file_location):
    json_data = {'your service account json data'}

    with open(key_file_location, 'w') as outfile:
        json.dump(json_data, outfile)


def delete_json_file(key_file_location):
    if os.path.exists(key_file_location):
        os.remove(key_file_location)


def get_goals(service, ids, start_date, end_date, metrics, dimensions):
    goals = service.data().ga().get(
        ids=ids,
        start_date=start_date,
        end_date=end_date,
        metrics=metrics,
        dimensions=dimensions
    ).execute()
    return goals


# --------------------------------Main
create_json_file(key_file_location)
# Authenticate and construct service.
service = get_service(
    api_name='analytics',
    api_version='v3',
    scopes=[scope],
    key_file_location=key_file_location)

# get all goals matrics and dimensions, should have done that in a for loop using the amount of goals there are.
goals = get_goals(service, ids, start_date, end_date, metric_completion + ',' + metric_conv_rate, '')

df = df.append({'start_time_utc': start_date,
                'Account_Creation_Step1_Completions': goals['totalsForAllResults']['ga:goal1Completions'],
                'Account_Creation_Step2_Completions': goals['totalsForAllResults']['ga:goal2Completions'],
                'Account_Creation_Step3_Completions': goals['totalsForAllResults']['ga:goal3Completions'],
                'Goal_Completions': goals['totalsForAllResults']['ga:goalCompletionsAll'],
                'Conversion_Rate_1': goals['totalsForAllResults']['ga:goal1ConversionRate'],
                'Conversion_Rate_2': goals['totalsForAllResults']['ga:goal2ConversionRate'],
                'Conversion_Rate_3': goals['totalsForAllResults']['ga:goal3ConversionRate'],
                'Conversion_Rate_All': goals['totalsForAllResults']['ga:goalConversionRateAll']}, ignore_index=True)

delete_json_file(key_file_location)
coolaResult = df
