impdG9rOmU5ZmU3YTNmX2MyZjBfNGM4OV9iNWNiXzUwOTA4ZmU1ZTc5NzoxOjAort requests, json, pandas as pd, datetime
#------Configurations------
headers =  {'Accept': 'application/json', 'Authorization':'Bearer <YOUR TOKEN>'}
#---dataframe---#
dfmsgs = pd.DataFrame(columns=['InsertTimeRecordUTC','conversation_id','msg_id','msg_type','msg_body','msg_created_at','author_type','author_id'])
today = datetime.date.today().strftime('%Y-%m-%d')
#--------------------------------Conversation Messages
r = requests.get("https://api.intercom.io/conversations",headers = headers).text
json_data = json.loads(r.strip())
for i in json_data['conversations']:
    r = requests.get("https://api.intercom.io/conversations/"+i['id'],headers = headers).text
    conv_data = json.loads(r.strip())
    for j in conv_data['conversation_parts']['conversation_parts']:
        if datetime.datetime.fromtimestamp(j['created_at']).strftime('%Y-%m-%d %H:%M:%S') > today:
            dfmsgs = dfmsgs.append({'InsertTimeRecordUTC'   : today,
                                    'conversation_id'       : i['id'] ,
                                    'msg_id'                : j['id'],
                                    'msg_type'              : j['part_type'],
                                    'msg_body'              : j['body'],
                                    'msg_created_at'        : datetime.datetime.fromtimestamp(j['created_at']).strftime('%Y-%m-%d %H:%M:%S'),
                                    'author_type'           : j['author']['type'],
                                    'author_id'             : j['author']['id']
                                    },ignore_index=True)

coolaResult = dfmsgs