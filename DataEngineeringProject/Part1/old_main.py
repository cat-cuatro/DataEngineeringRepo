import urllib.request
import subprocess
from datetime import date
import requests
import json

def grabBreadCrumbs(link="http://psudataeng.com:8000/getBreadCrumbData"):
    print('Requesting:', link)
    res = requests.get(link)
    status = res.status_code
    print('Request Status:', status)
    #print(res.json())
    current_date = generateDate()
    json_data = json.dumps(res.json())
    with open(current_date+".json", "w") as output:
        output.write(json_data)
    output.close()
    with open(current_date+"-ascii", "w") as asciioutput:
        asciioutput.write(res.text)
    asciioutput.close()
    print(res)

def generateDate():
    current_date = str(date.today())
    return current_date

def grabBreadCrumbsOS(link="http://psudataeng.com:8000/getBreadCrumbData"):
    current_date = generateDate()
    cmd = "curl -L "+ link +" --output "+current_date
    out = subprocess.check_output(cmd)
    return out, link

def main():
    out, url = grabBreadCrumbsOS()
    print('Process success status:', out)
    print('Yoinked data from:', url,'!')


if __name__ == "__main__":
    grabBreadCrumbs()
    #main()
