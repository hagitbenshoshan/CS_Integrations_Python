# This code brings inbound data from OracleBronto’s WSDL API to Cooladata

import suds
from suds.client import Client
from suds import WebFault
import logging
from datetime import datetime, date, time, timedelta

reload(sys)

import pandas as pd


# creating the dataframe to be imported to Cooladata
def df_manipulation_function(read_activity):
    local_read_activity = read_activity
    df = pd.DataFrame(
        columns=['createdDate', 'tz1', 'contactId', 'listId', 'messageId', 'deliveryId', 'activityType', 'emailAddress',
                 'contactStatus', 'messageName', 'deliveryType', 'deliveryStart', 'tz2', 'listName', 'listLabel'])

    for Accounts in local_read_activity:
        v_createdDate = ''
        v_tz1 = ''
        v_contactId = ''
        v_listId = ''
        v_messageId = ''
        v_deliveryId = ''
        v_activityType = ''
        v_emailAddress = ''
        v_contactStatus = ''
        v_messageName = ''
        v_deliveryType = ''
        v_deliveryStart = ''
        v_tz2 = ''
        v_listName = ''
        v_listLabel = ''

        if hasattr(Accounts, 'createdDate'):
            v_createdDate = str(Accounts.createdDate)[:19]
            v_tz1 = 0.0  # v_tz1 = str(Accounts.createdDate)[19:]

        if hasattr(Accounts, 'contactId'):
            v_contactId = Accounts.contactId.encode('utf-8').strip()

        if hasattr(Accounts, 'listId'):
            v_listId = Accounts.listId.encode('utf-8').strip()

        if hasattr(Accounts, 'messageId'):
            v_messageId = Accounts.messageId.encode('utf-8').strip()

        if hasattr(Accounts, 'deliveryId'):
            v_deliveryId = Accounts.deliveryId.encode('utf-8').strip()

        if hasattr(Accounts, 'activityType'):
            v_activityType = Accounts.activityType.encode('utf-8').strip()

        if hasattr(Accounts, 'emailAddress'):
            v_emailAddress = Accounts.emailAddress.encode('utf-8').strip()

        if hasattr(Accounts, 'contactStatus'):
            v_contactStatus = Accounts.contactStatus.encode('utf-8').strip()

        if hasattr(Accounts, 'messageName'):
            v_messageName = Accounts.messageName.encode('utf-8').strip()

        if hasattr(Accounts, 'deliveryType'):
            v_deliveryType = Accounts.deliveryType.encode('utf-8').strip()

        if hasattr(Accounts, 'deliveryStart'):
            v_deliveryStart = str(Accounts.deliveryStart)[:19]
            v_tz2 = 0.0  # v_tz2 = str(Accounts.deliveryStart)[19:]

        if hasattr(Accounts, 'listName'):
            v_listName = Accounts.listName.encode('utf-8').strip()

        if hasattr(Accounts, 'listLabel'):
            v_listLabel = Accounts.listLabel.encode('utf-8').strip()

        df = df.append({'createdDate': v_createdDate,
                        'tz1': v_tz1,
                        'contactId': v_contactId,
                        'listId': v_listId,
                        'messageId': v_messageId,
                        'deliveryId': v_deliveryId,
                        'activityType': v_activityType,
                        'emailAddress': v_emailAddress,
                        'contactStatus': v_contactStatus,
                        'messageName': v_messageName,
                        'deliveryType': v_deliveryType,
                        'deliveryStart': v_deliveryStart,
                        'tz2': v_tz2,
                        'listName': v_listName,
                        'listLabel': v_listLabel
                        }, ignore_index=True)

    return df


# OracleBronto API WSDL
BRONTO_WSDL = 'https://api.bronto.com/v4?wsdl'

# start up basic logging
logging.basicConfig()
TOKEN = "<Insert Token here>"

# login using the token to obtain a session ID
bApi = Client(BRONTO_WSDL)

try:
    # Use an existing session ID if you have one, otherwise, login
    # and obtain a new session ID
    session_id = ""
    session_id = bApi.service.login(TOKEN)
# exit if something goes wrong
except WebFault, e:
    print '\nERROR MESSAGE:'
    print e
    sys.exit()

from datetime import date
from datetime import datetime

today = date.today()
d = datetime(
    year=today.year,
    month=today.month,
    day=today.day,
)
todayts = datetime.utcnow().date()
until_dt = todayts - timedelta(days=0)
# Set up the soap headers using the
# session_id obtained from login()
session_header = bApi.factory.create("sessionHeader")
session_header.sessionId = session_id
bApi.set_options(soapheaders=session_header)

# Create the recentInboundActivitySearchRequest passed into
# readRecentInboundActivities()
# readLists


readDirection = bApi.factory.create('readDirection')
readDirection = "FIRST"
session_header = bApi.factory.create("sessionHeader")
session_header.sessionId = session_id
bApi.set_options(soapheaders=session_header)

# Create the recentInboundActivitySearchRequest passed into
# readRecentInboundActivities()
begining_of_yesterday = str(todayts) + ' 00:00:00.000000'
midnight = datetime.combine(date.today(), time.min)
yesterday_midnight = midnight - timedelta(days=1)

filter = bApi.factory.create('recentInboundActivitySearchRequest')
readDirection = bApi.factory.create('readDirection')
readDirection = "FIRST"

# Read data starting from 1 days ago up to now
filter.start = yesterday_midnight
filter.end = midnight
print
filter.end
print
filter.start
filter.size = 5000
filter.readDirection = readDirection
# Only return data for opens and clicks
filter.types = ['bounce', 'contactSkip', 'open', 'click', 'conversion', 'reply', 'unsubscribe', 'friendforward',
                'social', 'webform', 'sms_bounce', 'sms_reply']
# Initialize our countersprint filter
dt = filter.start

# Create empty DataFrame
df = pd.DataFrame(
    columns=['createdDate', 'contactId', 'listId', 'messageId', 'deliveryId', 'activityType', 'emailAddress',
             'contactStatus', 'messageName', 'deliveryType', 'deliveryStart', 'listName', 'listLabel'])
df1 = pd.DataFrame(
    columns=['createdDate', 'contactId', 'listId', 'messageId', 'deliveryId', 'activityType', 'emailAddress',
             'contactStatus', 'messageName', 'deliveryType', 'deliveryStart', 'listName', 'listLabel'])

i = 1
# Only get 100 pages worth of data
while i <= 100:

    if i == 1:
        print "Reading data for page 1 \n"
        try:
            filter.readDirection = "FIRST"
            read_activity = bApi.service.readRecentInboundActivities(filter)
        df = df_manipulation_function(read_activity)
        df1 = df1.append(df)
    except WebFault, e:
    print '\nERROR MESSAGE:'
    print e
else:
    print "Reading data for page " + str(i) + "\n"
try:
    filter.readDirection = "NEXT"

    read_activity = bApi.service.readRecentInboundActivities(filter)

    df = df_manipulation_function(read_activity)
    df1 = df1.append(df)
except WebFault, e:
    print '\nERROR MESSAGE:'
    print e
    print "No data on page " + str(i)
i = i + 1

coolaResult = df1


