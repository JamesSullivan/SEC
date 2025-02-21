import requests
import pandas as pd

def get_company_facts(cik):
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    headers = {
        "User-Agent": "Your Name (your@email.com)"
    }
    response = requests.get(url, headers=headers)
    return response.json()

def extract_financial_data(facts, concept):
    if concept in facts['facts']['us-gaap']:
        data = facts['facts']['us-gaap'][concept]['units']['USD']
        df = pd.DataFrame(data)
        df['end'] = pd.to_datetime(df['end'])
        df = df.sort_values('end', ascending=False)
        return df[['end', 'val']]
    return pd.DataFrame()

# Example usage
cik = "0001045810"  # CIK for Shopify Inc.
facts = get_company_facts(cik)

# Extract revenue data
revenue = extract_financial_data(facts, 'Revenue')
print(revenue.head())

# Extract net income data
net_income = extract_financial_data(facts, 'NetIncomeLoss')
print(net_income.head())
