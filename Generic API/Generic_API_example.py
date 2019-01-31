import requests
from datetime import datetime
import json
import pandas as pd

# import player data from internal api


# create an empty dataframe
# it serves as a container for the data
# that will be uploaded into Cooladata
df = pd.DataFrame(
    columns=['username',
             'Skin',
             'Email',
             'insert_time'])


# get the data and convert it to json
url = "http://www.example.com/get_users"
data = requests.get(url).text
json_data = json.loads(data.strip())

# insert the data into dataframe
for userDict in json_data:
    v_username = userDict['Username']
    v_Skin = userDict['Skin']
    v_Email = userDict['Email']
    v_insert_time = datetime.utcnow()

    df = df.append({'username': v_username,
                    'Skin': v_Skin,
                    'Email': v_Email,
                    'insert_time': v_insert_time
                    }, ignore_index=True)

# assign the result to "coolaResult" variable
# which will be uploaded into CoolaData
coolaResult = df

