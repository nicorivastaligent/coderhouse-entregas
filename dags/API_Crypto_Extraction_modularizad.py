from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from airflow import DAG
from io import StringIO
import pandas as pd
import requests
import psycopg2
import datetime
from datetime import datetime, timedelta
import json
import os

dag_path = os.getcwd()

##Seteo los argumentos defaul como email, los reintentos(retries) de ejecucion y la cadencia de reintentos(retry_delay)
default_args = {
    'owner': 'NicolasRivas',
    'start_date': datetime(2023,6,26),
    'retries':5,    
    'retry_delay': timedelta(minutes=5),
    'email': ['nicolas.rivas@taligent.com.ar'],
    'email_on_failure': True,
    'email_on_retry': True
}

##Defino el dag, seteo el intervalo de ejecucion(schedule_interval) y la funcion de catchup
BC_dag = DAG(
    dag_id='Bitcoin_ETL',
    default_args=default_args,
    description='Agrega data de Bitcoin de forma diaria',
    schedule_interval="@daily",
    catchup=False
)

##Task para la extracción de los datos desde la API publica
def extraer_data(**kwargs):
    url = "https://api.coincap.io/v2/assets"
    response = requests.get(url)  
    ### Transformmo el JSON de respuesta de la api en un dataframe para manejar los datos
    body_dict = response.json()
    data_json = body_dict['data']
    df = pd.DataFrame(data_json)
    ### Reetorna el valor del pythonoperator como json
    kwargs['ti'].xcom_push(key='data', value=df.to_json())

##Tranfsormo los datos para tener la informacion util
##Armo la que sera la primary key "ID de crypto-moneda | Fecha de consulta de infórmacion"
def transformar_data(**kwargs):

    ##Obtengo la data pasada de la funcion anterior
    ti = kwargs['ti']
    df_json = ti.xcom_pull(task_ids='extraer_data', key='data')
    df = pd.read_json(StringIO(df_json))

    ##Convierto el timestamp a string para concatenarlo a la PK    
    df['date_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    ##Armo la PK con el ID y el timestamp
    df['key_id_date_time'] = df['id'] + "|" + df['date_time']
    
    ##Armo tabla final con el DF
    df_final = df[['id', 'name', 'priceUsd', 'date_time', 'key_id_date_time']]

    ##Armo la ruta en la que se guardara el CSV con la informacion del DF para no pullear el DF entero y luego pulleo el path para tomarlo desde otra task
    file_path = f"/opt/airflow/dags/tmp_files/data{datetime.today()}.csv"
    df_final.to_csv(file_path, index=False)
    kwargs['ti'].xcom_push(key='csv_path', value=file_path)
    

##Task para cargar los datos en el DW en modalidad incremental (fijandose que no exista la misma PK para no duplicar información)
def cargar_data(**kwargs):
   ##Obtengo el path del CSV con la información pulleado desde la funcion anterior y leo dicho archivo
   data_file_path = kwargs['ti'].xcom_pull(task_ids='data_transformation', key='csv_path')
   df_final = pd.read_csv(data_file_path, delimiter=',')

   ## Traigo las credenciales de las variables de entorno
   load_dotenv()
   DB_USER = os.getenv('POSTGRES_USER')
   DB_PASS = os.getenv('POSTGRES_PASSWORD')
   DB_HOST = os.getenv('DB_HOST')
   DB_NAME = os.getenv('POSTGRES_DB')
   ##Creo el conector a la base de datos con las credenciales previamente leidas
   conn = psycopg2.connect(
       host=DB_HOST,
       dbname=DB_NAME,
       user=DB_USER,
       password=DB_PASS,
       port='5439'
   )

   ##Inicializo el cursor 
   cur = conn.cursor()
    
   ##Tabla temporal para almacenar los nuevos datos
   new_data_table_name = 'nicolas_rivas_coderhouse.cripto_price_new_data'

   ##Tabla destino en la que vamos a persistir los datos
   destiny_table_name = 'nicolas_rivas_coderhouse.cripto_price_history'
   ##Defino las columnas
   columns = ['id', 'name', 'priceUsd', 'date_time', 'key_id_date_time']
   values = [tuple(x) for x in df_final.to_numpy()]
   ######En las siguientes lineas se definen todas las queries necesarias a ejecutar.
   ##Elimino datos viejos de la temporal
   truncate_table_query = f"truncate table {new_data_table_name}"
   ##Cargo datos actualizados de la api en la temporal
   insert_data_in_temp_query = f"insert into {new_data_table_name} ({','.join(columns)}) values%s"
   ##Elimino duplicados de la temporal viendo que key es igual a la key de la tabla destino
   delete_duplicates_query = f"delete from {new_data_table_name} using {destiny_table_name} where {destiny_table_name}.key_id_date_time = {new_data_table_name}.key_id_date_time"
   ##inserto los datos de la temporal sin duplicados en la tabla destino
   final_insert_query = f"insert into {destiny_table_name} ({','.join(columns)}) select {','.join(columns)} from {new_data_table_name}"

   ##Ejecuto todas las queries en orden.
   cur.execute(truncate_table_query)
   cur.execute("BEGIN")
   execute_values(cur,insert_data_in_temp_query,values)
   cur.execute(delete_duplicates_query)
   cur.execute(final_insert_query)

   #Commiteo los cambios, cierro conexiones y cursor.
   cur.execute("COMMIT")
   cur.close()
   conn.close()

##Defino los pythonoperatros con las tasks previamente definidas y el emailoperator para mandar mail de aviso
email_operator = EmailOperator(
    task_id = 'send_email',
    to = 'nicolas.rivas@taligent.com.ar',
    subject = 'La base de datos de cryptos ha sido actualizada.',
    html_content = None,
    dag=BC_dag
)
data_extraction = PythonOperator(
    email_on_failure=True,
    task_id='extraer_data',
    python_callable=extraer_data,
    dag=BC_dag,
)

data_transformation = PythonOperator(
    email_on_failure=True,
    task_id='data_transformation',
    python_callable=transformar_data,
    dag=BC_dag,
)

data_load = PythonOperator(
    email_on_failure=True,
    task_id='data_load',
    python_callable=cargar_data,
    dag=BC_dag,
)

data_extraction >> data_transformation >> data_load >> email_operator