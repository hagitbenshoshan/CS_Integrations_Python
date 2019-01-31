import requests
import xml.etree.ElementTree as ET
import pandas as pd
import datetime

USER_NAME = 'your_username'
PASSWORD = 'your_password'

class XML2DataFrame:

    def __init__(self, xml_data):
        self.root = ET.XML(xml_data)

    def parse_root(self, root):
        return [self.parse_element(child) for child in iter(root)]

    def parse_element(self, element, parsed=None):
        if parsed is None:
            parsed = dict()

        for key in element.keys():

            if key == 'ID':
                if element.attrib.get(key) <> '3':
                    if element.attrib.get(key) <> '0':
                        parsed[key] = element.attrib.get(key)

        if element.text:
            parsed[element.tag] = element.text

        for child in list(element):
            self.parse_element(child, parsed)
        return parsed

    def process_data(self):
        structure_data = self.parse_root(self.root)
        return pd.DataFrame(structure_data)


# df_res = pd.DataFrame()
mydate = str(datetime.date.fromordinal(datetime.date.today().toordinal() - 1))  # Yesterday
user_agent_url = 'https://admin.secure.acraffiliates.com/feeds.php?FEED_ID=1' + '&JOIN_DATE=' + mydate

xml_data = requests.get(user_agent_url, auth=(USER_NAME, PASSWORD)).content
xml2df = XML2DataFrame(xml_data)
xml_dataframe = xml2df.process_data()

coolaResult = xml_dataframe