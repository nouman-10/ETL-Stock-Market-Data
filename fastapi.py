from fastapi import FastAPI, HTTPException
from google.cloud import bigquery
from dotenv import load_dotenv
import os

load_dotenv()

app = FastAPI()

# Initialize BigQuery client
client = bigquery.Client()

PROJECT_ID= os.environ.get("PROJECT_ID")
DATASET_ID = os.environ.get("DATASET_ID")
TABLE_ID = os.environ.get("TABLE_ID")

dataset_str = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

# Define endpoint to get stock data by stock name
@app.get("/stock/{stock_name}")
def get_stock_data(stock_name: str):
    query = f"""
        SELECT *
        FROM {dataset_str}
        WHERE stock_name = '{stock_name}'
    """
    try:
        # Execute the query
        query_job = client.query(query)
        results = query_job.result()
        
        # Convert results to list of dictionaries
        data = [dict(row) for row in results]
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    

# Endpoint to get stock data within a date range
@app.get("/stock/date_range/")
def get_stock_data_by_date_range(start_date: str, end_date: str):
    query = f"""
        SELECT *
        FROM {dataset_str}
        WHERE Date BETWEEN '{start_date}' AND '{end_date}'
    """
    try:
        # Execute the query
        query_job = client.query(query)
        results = query_job.result()
        # Convert results to list of dictionaries
        data = [dict(row) for row in results]
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    
# Endpoint to get stock data by price range
@app.get("/stock/price_range/")
def get_stock_data_by_price_range(min_price: float = None, max_price: float = None):
    condition = ""
    if min_price is not None and max_price is not None:
        condition = f"Price BETWEEN {min_price} AND {max_price}"
    elif min_price is not None:
        condition = f"Price >= {min_price}"
    elif max_price is not None:
        condition = f"Price <= {max_price}"
    
    query = f"""
        SELECT *
        FROM {dataset_str}
        WHERE {condition}
    """
    try:
        # Execute the query
        query_job = client.query(query)
        results = query_job.result()
        # Convert results to list of dictionaries
        data = [dict(row) for row in results]
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
