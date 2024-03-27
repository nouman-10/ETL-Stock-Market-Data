from google.cloud import bigquery
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = "YOUR-PROJECT-ID"
DATASET_ID = "YOUR-DATASET-ID" 
TABLE_ID = "YOUR-TABLE-ID"

client = bigquery.Client(project=PROJECT_ID)

dataset = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
dataset.location = "US"
from google.cloud.exceptions import NotFound

try:
    dataset = client.get_dataset(f"{PROJECT_ID}.{DATASET_ID}")  # Make an API request.
    print("Dataset {} already exists".format(DATASET_ID))
except NotFound:
    print("Dataset {} is not found".format(DATASET_ID))
    dataset = client.create_dataset(dataset, timeout=30)

schema = [
    bigquery.SchemaField("stock_name", "STRING"),
    bigquery.SchemaField("Date", "DATE"),
    bigquery.SchemaField("Open", "FLOAT"),
    bigquery.SchemaField("High", "FLOAT"),
    bigquery.SchemaField("Low", "FLOAT"),
    bigquery.SchemaField("Price", "FLOAT"),
    bigquery.SchemaField("VOL", "FLOAT"),
    bigquery.SchemaField("Change", "FLOAT"),
]

table_ref = dataset.table(TABLE_ID)
table = bigquery.Table(table_ref, schema=schema)

try:
    client.get_table(table)
    print("Table already exists")
except NotFound:
    table = client.create_table(table)
    print("Table created")

import pandas as pd

data = pd.read_csv("stock_data.csv")

def convert_to_float(x):
    if not x or x == 'nan':
        return None
    
    if x.endswith('M'):
        return float(x[:-1]) * 1e6
    elif x.endswith('B'):
        return float(x[:-1]) * 1e9
    else:
        return float(x)

data['Date'] = pd.to_datetime(data['Date'])
data['Price'] = data['Price'].str.replace(',', '').astype(float)
data['Open'] = data['Open'].str.replace(',', '').astype(float)
data['High'] = data['High'].str.replace(',', '').astype(float)
data['Low'] = data['Low'].str.replace(',', '').astype(float)


data.rename(columns={'Change': 'Change'}, inplace=True)
data['Change'] = data['Change'].str.rstrip('%').astype(float)

data['Vol.'] = data['Vol.'].astype(str)
data.rename(columns={'Vol.': 'Vol'}, inplace=True)
data['Vol'] = data['Vol'].apply(convert_to_float)

data.head()


# Load data from DataFrame to BigQuery
job_config = bigquery.LoadJobConfig()
job = client.load_table_from_dataframe(data, table_ref, job_config=job_config)

job.result()
