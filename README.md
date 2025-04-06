## Partially free closed source

[The easiest way to search and analyze SEC EDGAR filings](https://sec-api.io/)

[Financial Modeling Prep: Free Stock Market API and Financial Statements API](https://site.financialmodelingprep.com/developer/docs/financial-statement-free-api)

[Macrotrends](https://www.macrotrends.net/stocks/charts/GOLD/barrick-gold/financial-statements#google_vignette)

[PublicView product](https://www.publicview.ai/chat)

## Open Source
[Arelle is an end-to-end open source XBRL platform](https://arelle.readthedocs.io/en/2.21.3/index.html)

[py-xbrl](https://py-xbrl.readthedocs.io/en/latest/)

[Brel -  Python library for reading and analyzing financial reports](https://brellibrary.github.io/brel/)

[Extract information from financial statements from XBRL files in Python?](https://stackoverflow.com/questions/78207390/extract-information-from-financial-statements-from-xbrl-files-in-python)

[Edgar-and-the-Python](https://github.com/Peter-Staadecker/Edgar-and-the-Python)

[sec-edgar-scraper](https://github.com/Ameykolhe/sec-edgar-scraper/tree/main)

[SEC API - A SEC.gov EDGAR Filings Query & Real-Time Stream API](https://github.com/janlukasschroeder/sec-api-python)

[EDGAR_scraper](https://github.com/ahmetybesiroglu/EDGAR_scraper)

[edgar_db_tools](https://github.com/henrystern/edgar_db_tools/tree/main)




## SEC

[DERA Data Library](https://www.sec.gov/about/divisions-offices/division-economic-risk-analysis/dera-data-library)

[Financial Statement and Notes Data Sets](https://www.sec.gov/data-research/sec-markets-data/financial-statement-notes-data-sets)

## Database,

adsh 20 char accession (submission) number

sub  (submissions)
- adsh (pk)-> ren, num txt, pre, cal
- cik (important) company identifier

pre (presentation)
- adsh + report + line (pk)
- adsh -> sub, ren, num, txt, cal
- adsh, tag, version -> num
- tag (field as reported) 
- plabel (field standardized)

```sql
-- query to get income statement for submission 0000891618-09-000150
select * from pre where report = 2 (or statement = 'IS') and adsh = '0000891618-09-000150' order by line;
```
- 

## IFRS
- Financial position
- Comprehensive income
- Changes in equity
- Cash flows

---

- I     -  Statement of Income
- IP    -  Statement of Income (Parenthetical)
- CI    -  Statement of Comprehensive Income
- CIP   -  Statement of Comprehensive Income (Parenthetical)
- B     -  Balance Sheet
- BP    -  Balance Sheet (Parenthetical)
- C     -  Statement of Cash Flows
- CP    -  Statement of Cash Flows (Parenthetical)
- SE    -  Statement of Equity
- SEP   -  Statement of Equity (Parenthetical)



