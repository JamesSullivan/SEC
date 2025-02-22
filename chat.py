import json
import requests
import os
import pandas as pd
from typing import List

num: List[str] = ['0001164727', '0000002809', '0000764022', '0001340496', '0000883015', '0001430067', '0001695295', '0001009003', '0001158041', '0000701818']
jsonpath: str = "./json/"
def get_company_facts(cik):
    filename = f"{jsonpath}companyfacts/CIK{cik}.json"
    print(filename)
    # Check if the JSON file already exists
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            data = json.load(file)
            return data

    # If the file does not exist, make the request
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    headers = {
        "User-Agent": "James Sullivan (james.brian.sullivan@gmail.com)"
    }
    response = requests.get(url, headers=headers)
    data = response.json()

    # Save the results as a JSON file
    with open(filename, 'w') as file:
        json.dump(data, file)

    return data


def extract_financial_data(facts, concept):
    if concept in facts['facts']['us-gaap']:
        data = facts['facts']['us-gaap'][concept]['units']['USD']
        df = pd.DataFrame(data)
        df['end'] = pd.to_datetime(df['end'])
        df = df.sort_values('end', ascending=False)
        return df[['end', 'val']]
    return pd.DataFrame()

facts = get_company_facts(num[0])
print(facts)

# Extract revenue data
revenue = extract_financial_data(facts, 'Revenue')
print(revenue.head())

# Extract net income data
net_income = extract_financial_data(facts, 'NetIncomeLoss')
print(net_income.head())
