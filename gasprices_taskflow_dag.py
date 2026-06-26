from airflow.decorators import dag, task
from datetime import datetime, timedelta
import http.client
import os 
import json
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine

load_dotenv()

# Define the DAG using the @dag decorator
@dag(
   dag_id='gasprices_etl_dag_taskflow',
   start_date=datetime(2026, 6, 22),
   schedule=timedelta(minutes=1),
   catchup=False
)
def gasprices_etl_pipeline():

    # 1. EXTRACT TASK
    @task()
    def extract_gasprices(): 
        conn = http.client.HTTPSConnection("api.collectapi.com")
        headers = { 
            'content-type': "application/json", 
            'authorization': f"apikey {os.getenv('GASPRICES_API_KEY')}" 
        }
        conn.request("GET", "/gasPrice/stateUsaPrice?state=WA", headers=headers)

        res = conn.getresponse()
        data = res.read()
        gas_data = json.loads(data.decode("utf-8"))

        print(json.dumps(gas_data, indent=4))
        return gas_data

    # 2. TRANSFORM TASK
    @task()
    def transform_gasprices(city_data: dict):
        # Data automatically streams into 'city_data' parameter via TaskFlow
        cities_df = pd.DataFrame(city_data['result']['cities'])
        cities_df.head()
        cities_df = cities_df.drop(columns=['lowername'])
        cities_df = cities_df.rename(columns={'name': 'cities'})

        # Convert NaN values to None so Xcom can serialize it as JSON null
        cities_df = cities_df.astype(object).where(pd.notna(cities_df), None)
        
        cities_records = cities_df.to_dict(orient='records')
        return cities_records

    # 3. LOAD TASK
    @task()
    def load_gasprices(cities_records: list):
        # Data automatically streams into 'cities_records' parameter via TaskFlow
        cities_df = pd.DataFrame(cities_records) 

        DB_HOST = os.getenv('DB_HOST')
        DB_PORT = os.getenv('DB_PORT')
        DB_NAME = os.getenv('DB_NAME')
        DB_USER = os.getenv('DB_USER')
        DB_PASSWORD = os.getenv('DB_PASSWORD')

        engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        cities_df.to_sql('gasprices_pipeline', con=engine, if_exists='replace', index=False)

    # Define dependencies cleanly by passing the output of one function as the input of the next
    raw_data = extract_gasprices()
    cleaned_data = transform_gasprices(raw_data)
    load_gasprices(cleaned_data)

# Instantiate the DAG
gasprices_etl_pipeline()
