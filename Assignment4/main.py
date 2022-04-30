from cmath import nan
import pandas as pd
import re

def load():
    df = pd.read_csv('books.csv')
    df.drop(columns=
        [
            'Edition Statement', 
            'Corporate Author', 
            'Corporate Contributors', 
            'Former owner', 
            'Engraver', 
            'Issuance type', 
            'Shelfmarks'
        ]
    )
    print(df)
    return df

def findYear(row, subset=False):
    if isinstance(row, int):
        return
    if subset == False:
        matches = re.findall(r'(\d\d\d\d)', str(row))
        if len(matches) >= 2:
            largest = 0
            for e in matches:
                if largest < int(e):
                    largest = int(e)
            match = largest
        elif len(matches) == 1:
            match = matches[0]
        else:
            print(matches, 'no match)')
            match = float("nan")
    else:
        matches = re.findall(r'(\d\d)', str(row))
        if len(matches) == 3:
            match = int(''.join(str(matches[0])+str(matches[-1])))
        elif len(matches) >= 4:
            if matches[0] > matches[2]:
                match = int(''.join(str(matches[0])+str(matches[-1])))
            else:
                match = int(''.join(str(matches[2])+str(matches[-1])))
        else:
            match = matches[-1]
            match = findYear(row)
            #print(match)
    return match

def removeMisc(element):
    if '[' in element or ']' in element:
        element = element.lstrip('[')
        element = element.rstrip(']') 
    if '?' in element:
        element.rstrip('?')
    if '.' in element:
        element = element.lstrip('.')
        element = element.rstrip('.')
    return element

def cleanPublicationYear(df):
    for row in df['Date of Publication']:
        if '?' in str(row):
            row = float("nan")
        if '-' in str(row):
            row = findYear(row, subset=True)
        else:
            row = findYear(row)

    print(df['Date of Publication'])
    return df
    
def cleanPublicationCity(df):
    # How to use applymap?
    pass

def main():
    df = load()
    df = cleanPublicationYear(df)
    df = cleanPublicationCity(df)

if __name__ == "__main__":
    main()