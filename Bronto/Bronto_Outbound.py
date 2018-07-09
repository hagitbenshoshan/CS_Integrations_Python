#This code brings outbound data from Bronto’s WSDL API to Cooladata

import suds
from suds.client import Client
from suds import WebFault
import sys
import logging
from datetime import datetime, date, time, timedelta

import os, sys
import pandas as pd

# Bronto API WSDL
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

#creating the dataframe to be imported to Cooladata
mdf = pd.DataFrame(
    columns=['flag', 'createdDate', 'tz1', 'contactId', 'listId', 'messageId', 'deliveryId', 'activityType',
             'emailAddress', 'contactStatus', 'messageName', 'deliveryType', 'deliveryStart', 'tz2', 'listName',
             'listLabel'])


def daterange(beginning_of_period_midnight, midnight):
    import time
    for n in range(int((midnight - beginning_of_period_midnight).days)):
        cd = (beginning_of_period_midnight + timedelta(n))
        yield cd


todayts = datetime.utcnow().date()
begining_of_yesterday = str(todayts) + ' 00:00:00.000000'
midnight = datetime.combine(date.today(), time.min)
yesterday_midnight = midnight - timedelta(days=1)
beginning_of_period_midnight = midnight - timedelta(days=1)
until_dt = todayts - timedelta(days=0)

for single_date in daterange(beginning_of_period_midnight, midnight):
    # print single_date.strftime("%Y-%m-%d")
    #    manipulate_data(single_date)
    print(single_date)
    df = pd.DataFrame(
        columns=['flag', 'createdDate', 'tz1', 'contactId', 'listId', 'messageId', 'deliveryId', 'activityType',
                 'emailAddress', 'contactStatus', 'messageName', 'deliveryType', 'deliveryStart', 'tz2', 'listName',
                 'listLabel'])

    session_header = bApi.factory.create("sessionHeader")
    session_header.sessionId = session_id
    bApi.set_options(soapheaders=session_header)
    readDirection = bApi.factory.create('readDirection')
    filter = bApi.factory.create('recentOutboundActivitySearchRequest')
    # Read data starting from 1 days ago up to now
    filter.start = single_date
    print filter.start
    filter.size = 5000
    readDirection = "FIRST"
    filter.readDirection = readDirection
    # Only return data for opens and clicks
    filter.types = ['send']
    filter.end = datetime.combine(date.today(), time.min)
    # Initialize our countersprint filter
    dt = filter.start
    print "dt is :"
    print dt
    print filter

    i = 1
    # Only get 50000 pages worth of data
    while i <= 10:
        filter.start = single_date
        print filter.start
        if i == 1:
            print "Reading data for page 1 \n"
            try:
                readDirection = "FIRST"
                read_activity = bApi.service.readRecentOutboundActivities(filter)
            except WebFault, e:
                print '\nERROR MESSAGE:'
                print e
                i = i + 1
                continue  # sys.exit()
        else:

            print "Reading data for page " + str(i) + "\n"
            try:
                readDirection = "NEXT"
                filter.readDirection = readDirection
                read_activity = bApi.service.readRecentOutboundActivities(filter)
            except WebFault, e:
                print
                '\nERROR MESSAGE:'
                print
                e
                print
                "No data on page " + str(i)
                # out_file.close()
                # sys.exit()
                i = i + 1
                continue  # sys.exit()

        # df= pd.DataFrame(columns=['createdDate','contactId','listId','messageId','deliveryId','activityType','emailAddress','contactStatus','messageName','deliveryType','deliveryStart','listName','listLabel'])
        # print read_activity

        for Accounts in read_activity:
            v_createdDate = ''
            v_tz1 = 0.0
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
            v_tz2 = 0.0
            v_listName = ''
            v_listLabel = ''
            v_flag = ''

            if 9 == 9:

                if hasattr(Accounts, 'createdDate'):
                    v_createdDate = str(Accounts.createdDate)[:19]
                    # v_tz1=str(Accounts.createdDate)[19:]
                    if (str(Accounts.createdDate)[:10] < str(until_dt)[:10]):
                        v_flag = 'good'

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
                    # v_tz2=str(Accounts.deliveryStart)[19:]

                if hasattr(Accounts, 'listName'):
                    v_listName = Accounts.listName.encode('utf-8').strip()

                if hasattr(Accounts, 'listLabel'):
                    v_listLabel = Accounts.listLabel.encode('utf-8').strip()

                df = df.append({'flag': v_flag,
                                'createdDate': v_createdDate,
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

        i = i + 1
    print(df.shape)
    mdf = mdf.append(df)

# Create empty DataFrame


print(mdf.shape)
coolaResult = mdf
