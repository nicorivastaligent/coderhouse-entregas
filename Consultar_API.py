import requests
import json
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import datetime
from datetime import datetime

url = "https://api.coincap.io/v2/assets"
response = requests.get(url)

body_dict = response.json()
data_json = body_dict['data']
df = pd.DataFrame(data_json)
df['date_time'] = datetime.now()
df_final = df[['id', 'name', 'priceUsd', 'date_time']]

try:
    conn = psycopg2.connect(
        host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
        dbname='data-engineer-database',
        user='nicolas_rivas_coderhouse',
        password='hxA293BQGg',
        port='5439'
    )
except Exception as e:
    print("Unable to connect to Redshift.")
    print(e)

try:
    cur = conn.cursor()
    table_name = 'cripto_price_history'
    columns = ['id', 'name', 'price', 'date_time']
    values = [tuple(x) for x in df_final.to_numpy()]
    query = f"insert into {table_name} ({','.join(columns)}) values%s"
    cur.execute("BEGIN")
    execute_values(cur,query,values)
    
except Exception as e:
    conn.rollback()
    print('Unable to execute query.')
    print(e)

cur.execute("COMMIT")
cur.close()
conn.close()