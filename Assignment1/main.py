from gettext import npgettext
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from parso import parse
import seaborn as sns
from urllib.request import urlopen
from bs4 import BeautifulSoup
import re

PATTERN_KEYS = {
    'FirstLast': '[\\r|\\n]+\s+(.*)\\r\\n\\r\\n\s+',
    'Generic': '<td>(.+|)<\/td>',
    'TeamName': '[\\n|\\r]+\s+(.*)\\r\\n\s+'
}
STRUCTURE_KEYS = ['Name', 'Rank', 'Bib', 'Gender', 'City', 'Chip Time', 'Gun Time', 'Team Name']

def old(rows):
    for row in rows:
        nameMatch = re.findall(re.compile(PATTERN_KEYS['FirstLast']), str(row))
        print('Name Matches')
        for name in nameMatch:
            print(name)
        genericFields = re.findall(re.compile(PATTERN_KEYS['Generic']), str(row))
        print('Generic Matches')
        for field in genericFields:
            print(field)
        teamName = re.findall(re.compile(PATTERN_KEYS['TeamName']), str(row))
        print('Team Name Matches')
        for team in teamName:
            print(team)

def init():
    structure = {}
    for field in STRUCTURE_KEYS:
        structure.update({field: []})
    return structure

def updateStructure(structure, toAdd):
    for field in toAdd:
        structure[field].append(toAdd[field])

def retrievePage():
    url = "http://www.hubertiming.com/results/2017GPTR10K"
    print('Grabbing HTML page . .')
    html = urlopen(url)
    print('Done.')
    soup = BeautifulSoup(html, 'lxml')
    print(type(soup))
    return soup

def parseData(rows):
    """
    First attempt, but data wasn't in the form I needed and was missing too many elements.
    """
    participantStructuredData = init()
    for row in rows:
        for searchTerm in PATTERN_KEYS:
            pattern = re.compile(PATTERN_KEYS[searchTerm])
            print(row)
            print(pattern)
            match = re.findall(pattern, str(row))
            print(match)
            if searchTerm == 'FirstLast' and match:
                name = match[0]
                toAdd = {
                    'Name': name
                }
            elif searchTerm == 'Generic' and match:
                rank = match[0]
                bib = match[1]
                gender = match[2]
                city = match[3]
                chipTime = match[4]
                gunTime = match[5]
                toAdd = {
                    'Rank': rank,
                    'Bib': bib,
                    'Gender': gender,
                    'City': city,
                    'Chip Time': chipTime,
                    'Gun Time': gunTime,
                }
            elif searchTerm == 'TeamName' and match:
                teamName = match[0]
                toAdd = {
                    'Team Name': teamName
                }
            if match:
                updateStructure(participantStructuredData, toAdd)
    return participantStructuredData

def retrieveRunnerData(soup):
    title = soup.title
    print(title)
    text = soup.get_text()
    rows = soup.find_all('tr')
    for row in rows:
        field = str(row.find_all('td'))
        text = BeautifulSoup(field, 'lxml').get_text()
        data = text.split(',')
        if len(data) > 3:
            data[0] = data[0].lstrip('[')
            data[-1] = data[-1].rstrip(']')
            namePattern = re.compile(PATTERN_KEYS['FirstLast'])
            teamPattern = re.compile(PATTERN_KEYS['TeamName'])
            data[2] = re.findall(namePattern, data[2])[0]
            try:
                data[-1] = re.findall(teamPattern, data[-1])[0]
            except IndexError:
                pass
        for i in range(len(data)):
            if data[i] == ' ':
                data[i] = 'n/a'
        print(data)
    return pd.DataFrame(data)

# Ran into some hiccups with the original data pull (omitted code left above), 
# and I wasn't quite able to get the matplotlib stuff implemented in time before the end of the 2nd class period
# But at least I was able to learn some valuable experience with beautiful soup and relating that to HTML elements
def main():
    soup = retrievePage()
    print(retrieveRunnerData(soup))

if __name__ == "__main__":
    main()

## old
# <td>[\r\n]+[\s]+([a-zA-Z]+) ([a-zA-Z]+)[\r\n]+[\s]+<\/td> <<< First and last name
# <td>(.+|)<\/td> <<<    gender, city, chip time, gun time
# ^<img.*[\r\n]\s+(.*)$ <<<    Team name

## new
# [\\r|\\n]+\s+(.*)\\r\\n\\r\\n\s+ name
# [\\n|\\r]+\s+(.*)\\r\\n\s+ team name