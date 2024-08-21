import requests
import json
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import datetime
from datetime import datetime

url = "https://api.coincap.io/v2/assets"
response = requests.get(url)

### Transformmo el JSON de respuesta de la api en un dataframe para manejar los datos ###
body_dict = response.json()
data_json = body_dict['data']
df = pd.DataFrame(data_json)
### Fin transformacion a DF

##Convierto el timestamp a string para concatenarlo a la PK
df['date_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

##Armo la pk con el ID y el timestamp
df['key_id_date_time'] = df['id'] + "|" + df['date_time']

##Armo tabla final con el DF
df_final = df[['id', 'name', 'priceUsd', 'date_time', 'key_id_date_time']]

##Leo las credenciales de la DB
credentials = pd.read_csv('CREDENTIALS.csv', delimiter=',')
DB_USER = credentials['DB_USER'][0]
DB_PASS = credentials['DB_PASSWORD'][0]

##Creo el conector a la base de datos con las credenciales previamente leidas
conn = psycopg2.connect(
    host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
    dbname='data-engineer-database',
    user=DB_USER,
    password=DB_PASS,
    port='5439'
) 

##Creo el cursos, armo la tabla, convierto los datos en tuplpas para  luego armar la query y ejecutarla
cur = conn.cursor()
table_name = 'cripto_price_history'
columns = ['id', 'name', 'price', 'date_time', 'key_id_date_time']
values = [tuple(x) for x in df_final.to_numpy()]
query = f"insert into {table_name} ({','.join(columns)}) values%s"
cur.execute("BEGIN")
execute_values(cur,query,values)

##Commiteo los cambios, cierro conexiones y curso.
cur.execute("COMMIT")
cur.close()
conn.close()