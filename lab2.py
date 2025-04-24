# Importing 
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, date, timedelta
import snowflake.connector
import requests

# Defining snowflake connection 
def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


#############
## EXTRACT ##
#############

@task 
def extract(apikey, num_of_days, stock_symbol):

    # Define API endpoint
    API_URL = "https://www.alphavantage.co/query"

    # Last 180 days date range
    today = date.today()
    start_date = today - timedelta(days=num_of_days)

    # API request parameters
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": stock_symbol,
        "start_date": start_date.strftime("%Y-%m-%d"), "end_date": today.strftime("%Y-%m-%d"), "interval": "daily",
        "apikey": apikey 
        }

    # Send API request
    response = requests.get(API_URL, params=params)


    # Extract data
    if response.status_code == 200: 
        data = response.json() # Convert response to JSON print("Data retrieved successfully!")
        return data 

    else:
        print(f"Error: {response.status_code}, Message: {response.text}")

    

###############
## TRANSFORM ##
###############

@task 
def transform(input_data, num_of_days, stock_symbol):
    time_series = input_data.get("Time Series (Daily)", {})

    # Initialize 
    stock_data = []
    today = date.today()
    start_date = today - timedelta(days=num_of_days)  # Get last n days

    # Populate 
    for record_date, values in time_series.items():
        if datetime.strptime(record_date, "%Y-%m-%d").date() >= start_date:
            stock_data.append({
                "symbol": stock_symbol,
                "date": record_date,
                "open": float(values["1. open"]),
                "close": float(values["4. close"]),
                "high": float(values["2. high"]),
                "low": float(values["3. low"]),
                "volume": int(values["5. volume"])
            })

    return stock_data





##########
## LOAD ##
##########

@task 
def load(cursor, target_table, stock_data_input):
    try: 
        cursor.execute("BEGIN;")
        cursor.execute(f"""
            CREATE OR REPLACE TABLE user_db_marmot.dev.{target_table} ( 
            symbol STRING,
            date DATE,
            open FLOAT,
            close FLOAT,
            high FLOAT,
            low FLOAT,
            volume BIGINT,
            PRIMARY KEY (symbol, date)
            );
            """)
        cursor.execute(f"""DELETE FROM user_db_marmot.dev.{target_table}""")

        for item in stock_data_input:
            # Get all data 
            symbol = item['symbol']
            date = item['date']
            open = item['open']
            close = item['close']
            high = item['high']
            low = item['low']
            volume = item['volume']

            # Insert 
            sql = f"INSERT INTO user_db_marmot.dev.{target_table} (symbol, date, open, close, high, low, volume) VALUES ('{symbol}', '{date}', {open}, {close}, {high}, {low}, {volume})"
            cursor.execute(sql)

        cursor.execute("COMMIT;")

    except Exception as e:
        cursor.execute("ROLLBACK;") 
        print(e)
        raise(e)

#########
## ELT ##
#########

@task
def calculate_moving_averages(cursor, source_table, target_table):
    # This ELT task calculates the moving average of past 7 and 30 days and insert them into a new table 
    try:
        cursor.execute("BEGIN;")
        
        cursor.execute(f"""
            CREATE OR REPLACE TABLE user_db_marmot.dev.{target_table} (
                symbol STRING,
                date DATE,
                close FLOAT,
                ma_7 FLOAT,
                ma_30 FLOAT,
                PRIMARY KEY (symbol, date)
            );
        """)
        
        # Insert with moving averages (Snowflake SQL with window functions)
        cursor.execute(f"""
            INSERT INTO user_db_marmot.dev.{target_table}
            SELECT 
                symbol,
                date,
                close,
                AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma_7,
                AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS ma_30
            FROM user_db_marmot.dev.{source_table}
            ORDER BY symbol, date;
        """)

        cursor.execute("COMMIT;")
        
    except Exception as e:
        cursor.execute("ROLLBACK;")
        print("Error in ELT:", e)
        raise(e)




##########
## TASK ##
##########

# Connect to snowflake
cursor = return_snowflake_conn()

# Get API key from airflow 
api_key = Variable.get("api_key")

stock_symbol = "AAPL"
num_of_days = 180
target_table = "lab2_stock_data"


with DAG(
    dag_id = 'Lab2Task',
    start_date = datetime(2025,4,20),
    catchup=False,
    tags=['lab2'],
    schedule = '30 2 * * *'
) as dag:

    data = extract(api_key, num_of_days, stock_symbol)
    transformed_data = transform(data, num_of_days, stock_symbol)
    load_task = load(cursor, target_table, transformed_data)
    elt_task = calculate_moving_averages(cursor, target_table, "lab2_stock_moving_avg")
    
    # Dependency 
    data >> transformed_data >> load_task >> elt_task