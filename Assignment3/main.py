import pandas as pd

dataDictionary = {}
def buildHeader(keys):
    for key in keys:
        dataDictionary.update({key : []})
def loadFromFile(fname="Repo\DataEngineeringRepo\Assignment3\Oregon Hwy 26 Crash Data for 2019 - Crashes on Hwy 26 during 2019.csv"):
    f = open(fname)
    keys = f.readline()
    keys = keys.split(',')
    buildHeader(keys)
    for line in f.readlines():
        dataFields = line.split(',')
        i = 0
        for key in keys:
            try:
                val = int(dataFields[i])
            except ValueError:
                val = dataFields[i]
                val = val.rstrip('\n')
            dataDictionary[key].append(val)
            i += 1
    return pd.DataFrame(dataDictionary)

def existenceAssertion(df):
    selected_col = df['Crash ID'][1]
    print(selected_col)

def intraRecordAssertion(df):
    # Used for the intra record assertion
    allCrashIds = set(df['Crash ID'])
    for id in allCrashIds:
        selectedID = df[df['Crash ID'] == id]
        participantList = []
        vehicleList = []
        for participant in selectedID['Participant ID']:
            if isinstance(participant, int):
                participantList.append(participant)
        for vehicle in selectedID['Vehicle ID']:
            if isinstance(vehicle, int):
                vehicleList.append(vehicle)
        if len(set(vehicleList)) >= 2:
            if len(set(participantList)) < 2:
                print('Crash entry', id, 'has fewer than two participants for two or more vehicle IDs.')
                print(selectedID)
                print('Assert FAILED')
                return
    print('All crash entries have at least one participant per vehicle present.')
    print('Assert SUCCESS')

def limitAssertion(df):
    # Used for the limit assertion
    allCrashIds = set(df['Crash ID'])
    for id in allCrashIds:
        selectedID = df[df['Crash ID'] == id]
        count = 0
        for participant in selectedID['Participant ID']:
            if isinstance(participant, int):
                count += 1
        if count == 0:
            print('Crash entry', id, 'has no participants')
            print(selectedID)
            print('Assert FAILED')
            return
    print('All crash entries have been checked and contain participants.')
    print('Assert SUCCESS')

def main():
    df = loadFromFile()
    selected_id = df[df['Crash ID'] == 1809119]

    #existenceAssertion(df)
    #limitAssertion(df)
    #intraRecordAssertion(df)

    #print(selected_id.head())
    #for field in df:
    #    print(df[field] =='1809119')

if __name__ == "__main__":
    main()
