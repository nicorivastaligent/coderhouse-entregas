import requests
import json
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import datetime
import os
from airflow import DAG
from dotenv import load_dotenv
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'NicolasRivas',
    'start_date': datetime(2023,6,26),
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

BC_dag = DAG(
    dag_id='Bitcoin_ETL',
    default_args=default_args,
    description='Agrega data de Bitcoin de forma diaria',
    schedule_interval="@daily",
    catchup=False
)

def cargar_datos_a_base():
    url = "https://api.coincap.io/v2/assets"
    response = requests.get(url)

    ## Transformmo el JSON de respuesta de la api en un dataframe para manejar los datos ###
    body_dict = response.json()
    data_json = body_dict['data']
    df = pd.DataFrame(data_json)
    ## Fin transformacion a DF

    ##Convierto el timestamp a string para concatenarlo a la PK
    df['date_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    ##Armo la pk con el ID y el timestamp
    df['key_id_date_time'] = df['id'] + "|" + df['date_time']

    ##Armo tabla final con el DF
    df_final = df[['id', 'name', 'priceUsd', 'date_time', 'key_id_date_time']]

    ##Leo las credenciales de la DB
    load_dotenv('/opt/airflow/.env')
    DB_USER = os.getenv('POSTGRES_USER')
    DB_PASS = os.getenv('POSTGRES_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')
    DB_NAME = os.getenv('POSTGRES_DB')

    # #Creo el conector a la base de datos con las credenciales previamente leidas
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port='5439'
    ) 

    ##Creo el cursos, armo la tabla, convierto los datos en tuplpas para  luego armar la query y ejecutarla
    ##Por la naturaleza del formato de los datos (clave prima = 'id|fecha hh:mm:ss') nunca se repetira informacion en las ejecuciones.
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

cargar_data = PythonOperator(
    task_id='cargar_data',
    python_callable=cargar_datos_a_base,
    dag=BC_dag,
)

cargar_data