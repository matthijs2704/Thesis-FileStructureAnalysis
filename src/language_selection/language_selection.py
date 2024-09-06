from decimal import Decimal
import requests
from bs4 import BeautifulSoup
import requests
import time
import json

# from pyjsparser import parse
from calmjs.parse import es5
from calmjs.parse.unparsers.extractor import ast_to_dict


def getPYPLData() -> dict[str, Decimal]:
    url = "https://pypl.github.io/PYPL/All.js"
    response = requests.get(url)

    data = ast_to_dict(es5(response.text))["graphData"]
    headers = data[0][1:]
    last_data = data[-1][1:]
    pypl_data = dict(zip(headers, [round(x * 100, 2) for x in last_data]))
    pypl_data = dict(sorted(pypl_data.items(), key=lambda item: item[1], reverse=True))
    # print(pypl_data)
    return pypl_data


def getStackOverflowSurvey2023Data() -> dict[str, Decimal]:
    url = "https://survey.stackoverflow.co/2023/#technology"
    response = requests.get(url)

    soup = BeautifulSoup(response.content, "html.parser")

    # Print the title of the page
    # print(soup.title.string)

    # Find all the links on the page
    element = soup.find("figure", {"id": "most-popular-technologies-language"})
    overview_table = element.find("table")
    #
    table_rows = overview_table.find_all("tr")
    stackoverflow_data = {}
    for row in table_rows:
        label = row.find("td", {"class": "label"}).text.strip()
        percentage = round(
            float(row.find("td", {"class": "bar"})["data-percentage"]), 2
        )
        stackoverflow_data[label] = percentage

    # Combine C and C++ in stackoverflow_data as C/C++, to match PYPL
    if "C" in stackoverflow_data and "C++" in stackoverflow_data:
        stackoverflow_data["C/C++"] = stackoverflow_data.pop(
            "C"
        ) + stackoverflow_data.pop("C++")

    return stackoverflow_data


def getTiobeIndexData() -> dict[str, Decimal]:
    url = "https://www.tiobe.com/tiobe-index/"
    response = requests.get(url)

    soup = BeautifulSoup(response.content, "html.parser")

    # Print the title of the page
    # print(soup.title.string)

    # Find all the links on the page
    element = soup.find("table", {"id": "top20"})
    overview_table = element.find("tbody")
    #
    table_rows = overview_table.find_all("tr")
    tiobe_data = {}
    for row in table_rows:
        label = row.find_all("td")[4].text.strip()
        percentage = round(
            float(row.find_all("td")[5].text.strip().replace("%", "")), 2
        )
        tiobe_data[label] = percentage

    other_element = soup.find("table", {"id": "otherPL"})
    other_table = other_element.find("tbody")
    other_table_rows = other_table.find_all("tr")
    for row in other_table_rows:
        label = row.find_all("td")[1].text.strip()
        percentage = round(
            float(row.find_all("td")[2].text.strip().replace("%", "")), 2
        )
        tiobe_data[label] = percentage

    # print(tiobe_data)
    # Combine C and C++ in tiobe as C/C++, to match PYPL
    if "C" in tiobe_data and "C++" in tiobe_data:
        tiobe_data["C/C++"] = tiobe_data.pop("C") + tiobe_data.pop("C++")
    return tiobe_data


# Make the naming consistent across the datasets
mapping_dict = {
    "Bash": "Bash/Shell",
    "Julia": "Julia",
    "Delphi/Pascal": "Delphi",
    "Scala": "Scala",
    "PL/SQL": "PL/SQL",
    "Prolog": "Prolog",
    "Lisp": "Lisp",
    "Ruby": "Ruby",
    "Crystal": "Crystal",
    "Logo": "Logo",
    "Dart": "Dart",
    "Python": "Python",
    "Cobol": "COBOL",
    "CFML": "CFML",
    "Groovy": "Groovy",
    "F#": "F#",
    "Assembly": "Assembly",
    "OCaml": "OCaml",
    "Nim": "Nim",
    "Scratch": "Scratch",
    "Rust": "Rust",
    "Visual Basic (.Net)": "Visual Basic (.NET)",
    "R": "R",
    "Swift": "Swift",
    "Flow": "Flow",
    "GAMS": "GAMS",
    "Visual Basic": "Visual Basic",
    "Transact-SQL": "Transact-SQL",
    "Erlang": "Erlang",
    "C#": "C#",
    "Powershell": "PowerShell",
    "Classic Visual Basic": "Classic Visual Basic",
    "SAS": "SAS",
    "C/C++": "C/C++",
    "Ada": "Ada",
    "PowerShell": "PowerShell",
    "Kotlin": "Kotlin",
    "Objective-C": "Objective-C",
    "(Visual) FoxPro": "Visual FoxPro",
    "Java": "Java",
    "Abap": "ABAP",
    "VBA": "VBA",
    "Raku": "Raku",
    "Delphi": "Delphi",
    "Haskell": "Haskell",
    "Assembly language": "Assembly",
    "Elixir": "Elixir",
    "Go": "Go",
    "Fortran": "Fortran",
    "VBScript": "VBScript",
    "Apex": "Apex",
    "Matlab": "MATLAB",
    "Perl": "Perl",
    "JavaScript": "JavaScript",
    "GDScript": "GDScript",
    "X++": "X++",
    "Zig": "Zig",
    "SQL": "SQL",
    "TypeScript": "TypeScript",
    "Clojure": "Clojure",
    "ML": "ML",
    "COBOL": "COBOL",
    "HTML/CSS": "HTML/CSS",
    "Bash/Shell (all shells)": "Bash/Shell",
    "Delphi/Object Pascal": "Delphi",
    "ABAP": "ABAP",
    "D": "D",
    "Lua": "Lua",
    "APL": "APL",
    "Scheme": "Scheme",
    "PHP": "PHP",
    "MATLAB": "MATLAB",
    "Solidity": "Solidity",
}


def update_keys(dataset, mapping_dict):
    updated_dataset = {}
    for dataset_key, dataset_value in dataset.items():
        mapped_key = mapping_dict.get(dataset_key, dataset_key)
        updated_dataset[mapped_key] = dataset_value
    return updated_dataset


def combineDatasets(pypl_data, stackoverflow_data, tiobe_data):
    pypl_data_order = {k: i + 1 for i, (k, v) in enumerate(pypl_data.items())}
    stackoverflow_data_order = {
        k: i + 1 for i, (k, v) in enumerate(stackoverflow_data.items())
    }
    tiobe_data_order = {k: i + 1 for i, (k, v) in enumerate(tiobe_data.items())}

    pcs = set(pypl_data_order) | set(stackoverflow_data_order) | set(tiobe_data_order)

    combined_data = {}
    for key in pcs:
        combined_data[key] = [
            pypl_data_order.get(key, 1000),
            stackoverflow_data_order.get(key, 1000),
            tiobe_data_order.get(key, 1000),
        ]
    # print(combined_data)

    sorted_combined_data = sorted(
        [(k, v) for k, v in combined_data.items()],
        key=lambda x: sum(x[1]),
        reverse=False,
    )

    return sorted_combined_data

def get_top_languages(n=10) -> list[str]:
    pypl_data = getPYPLData()
    stackoverflow_data = getStackOverflowSurvey2023Data()
    tiobe_data = getTiobeIndexData()

    pypl_data = update_keys(pypl_data, mapping_dict)
    stackoverflow_data = update_keys(stackoverflow_data, mapping_dict)
    tiobe_data = update_keys(tiobe_data, mapping_dict)

    # Sort the datasets by value
    pypl_data = dict(sorted(pypl_data.items(), key=lambda item: item[1], reverse=True))
    stackoverflow_data = dict(
        sorted(stackoverflow_data.items(), key=lambda item: item[1], reverse=True)
    )
    tiobe_data = dict(
        sorted(tiobe_data.items(), key=lambda item: item[1], reverse=True)
    )   

    combined = combineDatasets(pypl_data, stackoverflow_data, tiobe_data)
    return combined[:n]

if __name__ == "__main__":
    print ("Retrieving data...")
    pypl_data = getPYPLData()
    print("Retrieved PYPL data")
    stackoverflow_data = getStackOverflowSurvey2023Data()
    print("Retrieved StackOverflow data")
    tiobe_data = getTiobeIndexData()
    print("Retrieved Tiobe data")

    print("==================== All ====================")
    print(set(pypl_data) | set(stackoverflow_data) | set(tiobe_data))
    print("==================== End ====================")

    pypl_data = update_keys(pypl_data, mapping_dict)
    stackoverflow_data = update_keys(stackoverflow_data, mapping_dict)
    tiobe_data = update_keys(tiobe_data, mapping_dict)

    # Sort the datasets by value
    pypl_data = dict(sorted(pypl_data.items(), key=lambda item: item[1], reverse=True))
    stackoverflow_data = dict(
        sorted(stackoverflow_data.items(), key=lambda item: item[1], reverse=True)
    )
    tiobe_data = dict(
        sorted(tiobe_data.items(), key=lambda item: item[1], reverse=True)
    )

    print("==================== PYPL ====================")
    print(pypl_data)
    print("==================== StackOverflow ====================")
    print(stackoverflow_data)
    print("==================== Tiobe ====================")
    print(tiobe_data)
    print(
        "==================== Common (based on rank within dataset) ===================="
    )
    combined = combineDatasets(pypl_data, stackoverflow_data, tiobe_data)[:10]
    for element, values in combined:
        print(f"{element}: {values}")
    print("==================== End ====================")

    output_data = {
        "date_collected": time.strftime("%Y-%m-%d"),
        "top_10": list(map(lambda x: x[0], combined)),
    }

    with open("datasets/top_10_languages.json", "w") as f:
        json.dump(output_data, f)
