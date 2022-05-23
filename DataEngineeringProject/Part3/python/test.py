import os
import sys
import json
#sys.path.append(os.path.abspath(__file__) + "/..")
sys.path.append('/home/lorenz2/DataEngineeringRepo/DataEngineeringProject/Part1')
print(sys.path)
from fetcher import Fetcher

#dataFetcher = Fetcher()
#f = dataFetcher.grabBreadCrumbs(write=False)
f = open('/home/lorenz2/2022-04-16-ascii')
data = json.load(f)

print(type(data))
for i in range(10):
    print(type(data[i]))
    print(data[i])
