from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
import http.client
import os 
import json
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine

load_dotenv()

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
    gasprices = [gas_data['result']]

    # kwargs['ti'].xcom_push(key="extract", value=gas_data) # pushing data explicitly to xcom

    return gas_data

   
def transform_gasprices(**kwargs):

    city_data = kwargs['ti'].xcom_pull(task_ids="extract")

    cities_df = pd.DataFrame(city_data['result']['cities'])

    cities_df.head()

    cities_df = cities_df.drop(columns=['lowername'])

    cities_df = cities_df.rename(columns={'name': 'cities'})

    #Convert NaN values to None so Xcom can serialize it as JSON null
    cities_df = cities_df.astype(object).where(pd.notna(cities_df), None)

    cities_records = cities_df.to_dict(orient='records')

    kwargs['ti'].xcom_push(key="transform", value=cities_records) 

def load_gasprices(**kwargs):
    cities_records = kwargs['ti'].xcom_pull(key="transform")

    cities_df = pd.DataFrame(cities_records) 

    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')

    engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

    cities_df.to_sql('gasprices_pipeline', con=engine, if_exists='replace', index=False)

with DAG(
   'gasprices_etl_dag',
   start_date=datetime(2026, 6, 22),
   schedule=timedelta(minutes=1),
   catchup=False
) as dag:
   
   extract_task = PythonOperator(
      task_id='extract',
      python_callable=extract_gasprices
   )

   transform_task = PythonOperator(
      task_id='transform',
      python_callable=transform_gasprices 
   )

   load_task = PythonOperator(
       task_id='load',
       python_callable=load_gasprices    
   )
 
   extract_task >> transform_task >> load_task
