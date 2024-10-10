from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from datetime import datetime
import snowflake.connector
import requests

def return_snowflake_conn():
    user_id = Variable.get('snowflake_username')
    password = Variable.get('snowflake_password')
    account = Variable.get('snowflake_account')

    conn = snowflake.connector.connect(
        user=user_id,
        password=password,
        account=account,
        warehouse='compute_wh',
        database='dev'
    )
    return conn.cursor()

from airflow.decorators import task
import requests

@task
def extract():
    # Retrieve API key and URL template from Airflow Variables
    api_key = Variable.get('VANTAGE_API_KEY')
    url_template = Variable.get("url")
   
    # Define the symbol
    symbol = 'MSFT'
   
    # Format the URL with the desired symbol and API key
    url = url_template.format(symbol=symbol, vantage_api_key=api_key)
   
    # Make the API request
    response = requests.get(url)
    data = response.json()
   
    return data

@task
def transform(data):
    transformed_results = []
    if 'Time Series (Daily)' in data:
        for date, values in data['Time Series (Daily)'].items():
            transformed = {
                'date': date,
                'open': values['1. open'],
                'high': values['2. high'],
                'low': values['3. low'],
                'close': values['4. close'],
                'volume': values['5. volume']
            }
            transformed_results.append(transformed)
    return transformed_results

@task
def load(cur, transformed_data):
    symbol = 'MSFT'  # Use the same symbol as in the extract function
    target_table = "DEMO1.raw_data.stock_price"

    if not transformed_data:
        print("No data to load.")
        return

    try:
        cur.execute("BEGIN;")
        cur.execute(f"""
        CREATE OR REPLACE TABLE {target_table} (
          date DATE,
          open NUMBER,
          high NUMBER,
          low NUMBER,
          close NUMBER,
          volume NUMBER,
          symbol VARCHAR
        )
        """)

        insert_sql = f"""
        INSERT INTO {target_table} (date, open, high, low, close, volume, symbol)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        for record in transformed_data:
            cur.execute(insert_sql, (
                record['date'],
                record['open'],
                record['high'],
                record['low'],
                record['close'],
                record['volume'],
                symbol
            ))

        cur.execute("COMMIT;")
        print("Data loaded successfully.")

    except Exception as e:
        cur.execute("ROLLBACK;")
        print("An error occurred while loading data:", str(e))
        raise e

with DAG(
    dag_id='StockPriceETL',
    start_date=datetime(2024, 9, 21),
    catchup=False,
    tags=['ETL'],
    schedule='0 2 * * *'
) as dag:
    
    cur = return_snowflake_conn()
    
    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load(cur, transformed_data)