## John Lorenz IV // CS 510: Data Engineering // In Class Assignment 5 & Assignment 6
import pandas as pd
import psycopg2
import numpy as np

REGULAR_KEYS = [
    'ID',
    'State',
    'County',
]
SUMMING_KEYS = [
    'TotalPop',
    'Men',
    'Women',
    'Hispanic',
    'White',
    'Black',
    'Native',
    'Asian',
    'Pacific',
    'VotingAgeCitizen',
    'Employed',
]
AVERAGING_KEYS = [
    'Income',
    'IncomeErr',
    'IncomePerCap',
    'IncomePerCapErr',
    'Poverty',
    'ChildPoverty',
    'Professional',
    'Service',
    'Office',
    'Construction',
    'Production',
    'Drive',
    'Carpool',
    'Transit',
    'Walk',
    'OtherTransp',
    'WorkAtHome',
    'MeanCommute',
#    'Employed',
    'PrivateWork',
    'PublicWork',
    'SelfEmployed',
    'FamilyWork',
    'Unemployment',
]

def readData(file):
    storeIn = pd.read_csv(file)
    return storeIn

def sumDf(censusTractDf):
    return censusTractDf.sum()

def averageDf(censusTractDf):
    avg = censusTractDf.sum()/len(censusTractDf)
    return avg

def populateHeader(transformedDf):
    for regkey in REGULAR_KEYS:
        transformedDf.update({regkey: []})
    for sumkey in SUMMING_KEYS:
        transformedDf.update({sumkey: []})
    for avgkey in AVERAGING_KEYS:
        transformedDf.update({avgkey: []})

    return transformedDf

def pickState(df, visited, county):
    keys = list(df['State'])
    for key in keys:
        if key not in list(visited.keys()):
            visited.update({key : [county]})
            #print('Added..', key, ',', county)
            return df.loc[df['State'] == key]
        else:
            countiesVisited = visited[key]
            if county not in countiesVisited:
                visited[key].append(county)
                #print('Added..', key, ',', county)
                return df.loc[df['State'] == key]

def consolidateCensusData(df, transformedDf):
    #print(list(df.keys()))
    countyList = list(set(df['County']))
    count = 0
    visited = {}
    for county in countyList:
        tempdf = df.loc[df['County'] == county]
        tempdf = pickState(tempdf, visited, county)
        transformedDf['ID'].append(count)
        count += 1
        transformedDf['State'].append(set(tempdf['State']))
        transformedDf['County'].append(county)
        for sumkey in SUMMING_KEYS:
            sum = sumDf(tempdf[sumkey])
            transformedDf[sumkey].append(sum)
        for avgkey in AVERAGING_KEYS:
            avg = averageDf(tempdf[avgkey])
            transformedDf[avgkey].append(avg)
    
    #print(countyList[0])
    #tempdf = df.loc[df['County'] == countyList[0]]
    #print(tempdf)
    #print(tempdf['VotingAgeCitizen'])
    #print(list(tempdf.keys()))
    return transformedDf

def main():
    transformedDf = populateHeader({})
    print(transformedDf)
    covidDf = readData('COVID_county_data.csv')
    censusDf = readData('acs2017_census_tract_data.csv')
    transformedDf = consolidateCensusData(censusDf, transformedDf)
    print(pd.DataFrame.from_dict(transformedDf))
    #print(covidDf)
    #print(censusDf)

if __name__ == "__main__":
    main()