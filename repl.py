"""
Exracting Financial Statements from SEC Filings - XBRL-To-JSON

This is the entire Jupyter notebook to extract financial statements from annual and quarterly reports as reported in 10-K and 10-Q filings with the SEC.

We use https://sec-api.io to get all 10-K and 10-Q filings and to convert their XBRL data into JSON so that we can create a single income statement, balance sheet and cash flow statement for Apple, covering quarterly financial data over multiple years.

Medium article:
https://medium.com/@jan_5421/extracting-financial-statements-from-sec-filings-xbrl-to-json-f83542ade90
"""

# %% Cell 2
import os
import requests
import json

# get your free API key at https://sec-api.io
api_key: str = os.environ.get('SEC_API_KEY', '')
if len(api_key) < 1:
    raise ValueError("SEC_API_KEY is not set in the environment variables.")

# %% Cell 3
# 10-Q filing URL of Apple
filing_url = "https://www.sec.gov/Archives/edgar/data/1164727/000116472724000061/nem-20240930.htm"
# XBRL-to-JSON converter API endpoint
xbrl_converter_api_endpoint = "https://api.sec-api.io/xbrl-to-json"
final_url = xbrl_converter_api_endpoint + "?htm-url=" + filing_url + "&token=" + api_key
# make request to the API
response = requests.get(final_url)
# load JSON into memory
xbrl_json = json.loads(response.text)
# income statement example
print(json.dumps(xbrl_json['StatementsOfIncome']['RevenueFromContractWithCustomerExcludingAssessedTax'][0:2], indent=1))


# %% Cell 4

# %% Cell 5

# %% Cell 6
