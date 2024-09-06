from decimal import Decimal
import requests
from bs4 import BeautifulSoup
import requests
import re
import json
import ast
# from pyjsparser import parse
from calmjs.parse import es5
from calmjs.parse.unparsers.extractor import ast_to_dict

def getPYPLData():
    url = 'https://pypl.github.io/PYPL/All.js'
    response = requests.get(url)

    data = ast_to_dict(es5(response.text))["graphData"]
    headers = data[0][1:]
    last_data = data[-1][1:]
    pypl_data = dict(zip([h.lower() for h in headers], [round(x*100,2) for x in last_data]))
    pypl_data = dict(sorted(pypl_data.items(), key=lambda item: item[1], reverse=True))
    # print(pypl_data)
    return pypl_data

def getStackOverflowSurvey2023Data():
    url = 'https://survey.stackoverflow.co/2023/#technology'
    response = requests.get(url)

    soup = BeautifulSoup(response.content, 'html.parser')

    # Print the title of the page
    # print(soup.title.string)

    # Find all the links on the page
    element = soup.find('figure', {'id': 'most-popular-technologies-language'})
    overview_table = element.find('table')
    #
    table_rows = overview_table.find_all('tr')
    stackoverflow_data = {}
    for row in table_rows:
        label = row.find('td', {'class': 'label'}).text.strip().lower()
        percentage = round(float(row.find('td', {'class': 'bar'})['data-percentage']),2)
        stackoverflow_data[label] = percentage
    # print(stackoverflow_data)
    return stackoverflow_data


def getTiobeIndexData():
    url = 'https://www.tiobe.com/tiobe-index/'
    response = requests.get(url)

    soup = BeautifulSoup(response.content, 'html.parser')

    # Print the title of the page
    # print(soup.title.string)

    # Find all the links on the page
    element = soup.find('table', {'id': 'top20'})
    overview_table = element.find('tbody')
    #
    table_rows = overview_table.find_all('tr')
    tiobe_data = {}
    for row in table_rows:
        label = row.find_all('td')[4].text.strip().lower()
        percentage = round(float(row.find_all('td')[5].text.strip().replace('%', '')),2)
        tiobe_data[label] = percentage
    
    # print(tiobe_data)
    return tiobe_data

pypl_data = getPYPLData()
stackoverflow_data = getStackOverflowSurvey2023Data()
tiobe_data = getTiobeIndexData()

# BEGIN: yz18d9bcejpp
from fuzzywuzzy import fuzz

def combineDatasets():
    pypl_data = getPYPLData()
    stackoverflow_data = getStackOverflowSurvey2023Data()
    tiobe_data = getTiobeIndexData()

    # Combine C and C++ in stackoverflow_data and tiobe_data as C/C++
    if 'c' in stackoverflow_data and 'c++' in stackoverflow_data:
        stackoverflow_data['c/c++'] = stackoverflow_data.pop('c') + stackoverflow_data.pop('c++')
    if 'c' in tiobe_data and 'c++' in tiobe_data:
        tiobe_data['c/c++'] = tiobe_data.pop('c') + tiobe_data.pop('c++')

    # Perform fuzzy matching on the keys
    combined_data = {}
    for key in set(pypl_data.keys()) | set(stackoverflow_data.keys()) | set(tiobe_data.keys()):
        fuzzy_matches = []
        for pypl_key in pypl_data.keys():
            if fuzz.ratio(key, pypl_key) > 80:
                fuzzy_matches.append(pypl_key)
        for stackoverflow_key in stackoverflow_data.keys():
            if fuzz.ratio(key, stackoverflow_key) > 80:
                fuzzy_matches.append(stackoverflow_key)
        for tiobe_key in tiobe_data.keys():
            if fuzz.ratio(key, tiobe_key) > 80:
                fuzzy_matches.append(tiobe_key)
        if fuzzy_matches:
            combined_data[key] = [pypl_data.get(fuzzy_matches[0], 0), stackoverflow_data.get(fuzzy_matches[0], 0), tiobe_data.get(fuzzy_matches[0], 0)]
        else:
            combined_data[key] = [0, 0, 0]

    sorted_combined_data = sorted([(k, v) for k, v in combined_data.items() if all(v)], key=lambda x: sum(x[1]), reverse=True)

    return sorted_combined_data
    
    sorted_combined_data = sorted(combined_data.items(), key=lambda x: sum(x), reverse=True)[:10]
    # print ('==================== Combined Data ====================')
    # for element, values in sorted_combined_data:
    #     print(f"{element}: {values}")
    # print ('==================== End ====================')
    return sorted_combined_data

# END: yz18d9bcejpp
print ('==================== PYPL ====================')
print (pypl_data)
print ('==================== StackOverflow ====================')
print (stackoverflow_data)
print ('==================== Tiobe ====================')
print (tiobe_data)
print ('==================== Common ====================')
for element, values in combineDatasets()[:10]:
    print(f"{element}: {values}")
print ('==================== End ====================')