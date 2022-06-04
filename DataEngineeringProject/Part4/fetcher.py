"""
Author: John Lorenz IV
###
This source code is for an application that is a part of graduate coursework. 
This repository will be made private when the class is over.
###
Fetcher class object for querying data from a website.
Outputs two files: a 'json' version (which appears broken/not very useful), and an ascii version which is
structured exactly like a json file but is defined as an ascii file. Currently only using the ascii information.
###
"""
from datetime import date
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import re
import requests
import json

def generateDate():
    current_date = str(date.today())
    return current_date

def firstNumberInHeader(pattern, header):
    match = re.findall(pattern, header)
    return match[0]

class Fetcher():
    def __init__(self):
        self.PATTERN_KEYS = {
            'STOP_EVENT_ID': r'^<h3>[a-zA-Z ]+(\d+)'
        }
        self.stop_event_keys = [
            'STOP_EVENT_ID',
            'VEHICLE_NUMBER',
            'LEAVE_TIME',
            'TRAIN',
            'ROUTE_NUMBER',
            'DIRECTION',
            'SERVICE_KEY',
            'STOP_TIME',
            'ARRIVE_TIME',
            'DWELL',
            'LOCATION_ID',
            'DOOR',
            'LIFT',
            'ONS',
            'OFFS',
            'ESTIMATED_LOAD',
            'MAXIMUM_SPEED',
            'TRAIN_MILEAGE',
            'PATTERN_DISTANCE',
            'LOCATION_DISTANCE',
            'X_COORDINATE',
            'Y_COORDINATE',
            'DATA_SOURCE',
            'SCHEDULE_STATUS'
        ]

    def _clean(self, rows):
        for i in range(0, len(rows)):
            for j in range(0, len(rows[i])):
                temp = str(rows[i][j]).lstrip('<td>').rstrip('</td>')
                if temp:
                    rows[i][j] = temp
                else:
                    rows[i][j] = '0'
        return rows

    def _buildStopEventDataframe(self, id_to_rows):
        # Table fields and stop event ids assumed in-order.
        temporary_structure = {}
        for key in self.stop_event_keys:
            temporary_structure.update({key : []})
        for id in id_to_rows:
            cleaned_rows = self._clean(id_to_rows[id])
            for cleaned_row in cleaned_rows:
                idx = 0
                for key in self.stop_event_keys:
                    if key == 'STOP_EVENT_ID':
                        temporary_structure[key].append(id)
                    else:
                        temporary_structure[key].append(cleaned_row[idx])
                        idx += 1
        return pd.DataFrame.from_dict(temporary_structure)
    
    def grabStopEvents(self, write=True, link="http://www.psudataeng.com:8000/getStopEvents/"):
        current_date = generateDate()
        print('Requesting:', link)
        res = urlopen(link)
        html = BeautifulSoup(res, 'lxml')
        stop_event_ids = html.find_all('h3')
        idx = 0
        stop_event_data = html.find_all('table')
        id_to_rows = {}
        table_fields = []
        for event_table in stop_event_data:
            table_rows = event_table.find_all('tr')
            temp = []
            for row in table_rows:
                temp.append(row.find_all('td'))
            del temp[0] # I'm getting garbage data at this specific index (empty list)
            table_fields.append(temp)
            event_id = firstNumberInHeader(self.PATTERN_KEYS['STOP_EVENT_ID'], str(stop_event_ids[idx]))
            id_to_rows.update({event_id : temp})
            idx += 1
        print('There are', len(stop_event_ids), 'stop event IDs.')
        print('There are', len(table_fields), 'logged tables.')
        for x in range(0, len(stop_event_ids)):
            for row in table_fields[x]:
                try:
                    t = str(row[15]).lstrip('<td>').rstrip('</td>')
                    if t:
                        print("I've found a maximum velocity reading:", t)
                except IndexError:
                    pass
        stop_event_dataframe = self._buildStopEventDataframe(id_to_rows)
        return stop_event_dataframe
        ## Below is currently unused intentionally
        if write == True:
            out = stop_event_dataframe.to_json()
            fname = current_date+"-stopevents"
            with open(fname, "w") as output:
                output.write(out)
            return fname
        else:
            data = stop_event_dataframe
            return data

    def grabBreadCrumbs(self, write=True, link="http://psudataeng.com:8000/getBreadCrumbDataV2"):
        print('Requesting:', link)
        res = requests.get(link)
        status = res.status_code
        print('Request Status:', status)
        current_date = generateDate()
        json_data = json.dumps(res.json())
        print(type(json_data))
        if write == True:
            with open(current_date+".json", "w") as output:
                output.write(json_data)
            output.close()
            with open(current_date+"-ascii", "w") as asciioutput:
                asciioutput.write(res.text)
            asciioutput.close()
        print(res)
        fname = current_date+"-ascii"
        if write == True:
            return fname
        else:
            return res.json()


if __name__ == "__main__":
    fetcher = Fetcher()
    df = fetcher.grabStopEvents()
    print('Total of', len(df.index), 'entries.')
    newdf = df.iloc[[0]].to_json(orient='split', index=False)
    dictdf = df.iloc[[0]].to_dict(orient='index')
    print(dictdf[0])
    dictdf[0].update({'count': 1})
    print(json.dumps(dictdf[0]))
    #newdf.update({"count": 1})
    #crumb = fetcher.grabBreadCrumbs(write=False)
    #print(crumb[0])

