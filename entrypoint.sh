#!/bin/bash
set -e

#Instalo psycopg2 para poder utilizar la base
pip install psycopg2-binary

# Inicializar la base de datos
airflow db migrate

#Crear usuario admin si no existe
if ! airflow users list | grep -q admin; then
  airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email nicolas.rivas@taligent.com.ar
fi

# Iniciar el scheduler en segundo plano
airflow scheduler &

# Iniciar el webserver
exec airflow webserver
