import requests, json, pandas as pd, datetime

# ------Configurations------
headers = {'Accept': 'application/json',
           'Authorization': 'Bearer <YOUR TOKEN>'}
# ---dataframe---#
dfconv = pd.DataFrame(
    columns=['InsertTimeRecordUTC', 'id', 'created_at', 'updated_at', 'waiting_since', 'snoozed_until',
             'first_msg_body', 'first_msg_author_type', 'author_id',
             'user_id', 'admin_assignee_id', 'open', 'state', 'read', 'rating', 'rating_remark'])
today = datetime.date.today().strftime('%Y-%m-%d')


# ------Variable validations------
def validate_snooze(time):
    if time is None:
        return False
    return datetime.datetime.fromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')


def validate_waiting(conv):
    if (conv['open'] == False) or (conv['waiting_since'] > 60000000000):
        return False
    return datetime.datetime.fromtimestamp(conv['waiting_since']).strftime('%Y-%m-%d %H:%M:%S')


# ------Fetches------
def fetch_rating(fetch_type, convid):
    r = requests.get("https://api.intercom.io/conversations/" + convid, headers=headers).text
    json_data = json.loads(r.strip())
    return json_data['conversation_rating'][fetch_type]


# --------------------------------Get all conversation objects
r = requests.get("https://api.intercom.io/conversations", headers=headers).text
json_data = json.loads(r.strip())

for i in json_data['conversations']:
    dfconv = dfconv.append({'InsertTimeRecordUTC': today,
                            'id': i['id'],
                            'created_at': datetime.datetime.fromtimestamp(i['created_at']).strftime(
                                '%Y-%m-%d %H:%M:%S'),
                            'updated_at': datetime.datetime.fromtimestamp(i['updated_at']).strftime(
                                '%Y-%m-%d %H:%M:%S'),
                            'waiting_since': str(validate_waiting(i)),
                            'snoozed_until': str(validate_snooze(i['snoozed_until'])),
                            'first_msg_body': i['conversation_message']['body'],
                            'first_msg_author_type': i['conversation_message']['author']['type'],
                            'author_id': i['conversation_message']['author']['id'],
                            'user_id': i['user']['id'],
                            'admin_assignee_id': i['assignee']['id'],
                            'open': str(i['open']),
                            'state': i['state'],
                            'read': str(i['read']),
                            'rating': fetch_rating('rating', i['id']),
                            'rating_remark': fetch_rating('remark', i['id'])
                            }, ignore_index=True)

while json_data['pages']['next'] != None:
    r = requests.get(json_data['pages']['next'], headers=headers).text
    json_data = json.loads(r.strip())
    for i in json_data['conversations']:
        dfconv = dfconv.append({'InsertTimeRecordUTC': today,
                                'id': i['id'],
                                'created_at': datetime.datetime.fromtimestamp(i['created_at']).strftime(
                                    '%Y-%m-%d %H:%M:%S'),
                                'updated_at': datetime.datetime.fromtimestamp(i['updated_at']).strftime(
                                    '%Y-%m-%d %H:%M:%S'),
                                'waiting_since': str(validate_waiting(i)),
                                'snoozed_until': str(validate_snooze(i['snoozed_until'])),
                                'first_msg_body': i['conversation_message']['body'],
                                'first_msg_author_type': i['conversation_message']['author']['type'],
                                'author_id': i['conversation_message']['author']['id'],
                                'user_id': i['user']['id'],
                                'admin_assignee_id': i['assignee']['id'],
                                'open': str(i['open']),
                                'state': i['state'],
                                'read': str(i['read']),
                                'rating': fetch_rating('rating', i['id']),
                                'rating_remark': fetch_rating('remark', i['id'])
                                }, ignore_index=True)
coolaResult = dfconv
