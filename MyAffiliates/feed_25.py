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
            parsed[key] = element.attrib.get(key)
        if element.text:
            parsed[element.tag] = element.text
        for child in list(element):
            self.parse_element(child, parsed)
        return parsed

    def process_data(self):
        structure_data = self.parse_root(self.root)
        return pd.DataFrame(structure_data)


from_date = str(datetime.date.fromordinal(datetime.date.today().toordinal() - 1))  # Yesterday
to_date = str(datetime.date.fromordinal(datetime.date.today().toordinal() - 1))  # Yesterday

affiliates_feed25 = 'https://admin.secure.acraffiliates.com/feeds.php?FEED_ID=25' \
                    + '&FROM_DATE=' + from_date \
                    + '&TO_DATE=' + to_date

xml_data = requests.get(affiliates_feed25, auth=(USER_NAME, PASSWORD)).content
xml2df = XML2DataFrame(xml_data)
xml_dataframe = xml2df.process_data()

# add date column to result
xml_dataframe['earning_date_from'] = from_date
xml_dataframe['earning_date_to'] = to_date

coolaResult = xml_dataframe