# Fetch Modeling

Repo to ingest fetch receipt data, transform, and build DWH


### Initialize

 - Uses UV venv with python >=3.11
 - Use jupyter notebooks folder to extract files from URLs, ingest, and load data
 - Notebook contains business queries and EDA for QA callouts

### Stack
- python
- duckdb
- jupyter

### Modeling

The biggest modeling callout is breaking down receipts into receipt line items
This will have 2 fact tables, receipts and line items where receipts will act as an aggregate when details are not needed
you could advocate to allocate and put it all in line item but then some queries may get complex
similarly you could go with one big table, but may run into scaling and complexity with queries or a semantic model down the road

### Note to Stakeholders
Hi Super Important Stakeholder,

I was reviewing the data you sent over and had some question on a few of the columns and how to best interpret them:

 - I noticed the users file does not have distinct user_ids, I'm thinking we want to de duplicate them wanted to make sure there was not a specific reason for the duplicates

 - I was also confused on the brand file and which column to best use when tying together the receipt and brand data? I was assuming brand code, but it looks to be sparsely populated in the receipts data

	 - I noticed barcode too, but again it seems inconsistent

Looking forward, do you have any ideas of data size and how often we would need to refresh this? The current set up is in-memory since the volumes are low. But if we wanted to scale this we would need to build out an incremental loading process for the data and likely move it to a more robust DB.

Thanks,

Fetch Data Guys

