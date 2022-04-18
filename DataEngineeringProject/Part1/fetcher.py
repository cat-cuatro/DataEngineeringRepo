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
import requests
import json
def generateDate():
    current_date = str(date.today())
    return current_date
class Fetcher():
    def __init__(self):
        pass

    def grabBreadCrumbs(self, write=True, link="http://psudataeng.com:8000/getBreadCrumbData"):
        print('Requesting:', link)
        res = requests.get(link)
        status = res.status_code
        print('Request Status:', status)
        current_date = generateDate()
        json_data = json.dumps(res.json())
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
   pass 
