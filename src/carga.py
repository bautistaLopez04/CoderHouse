import psycopg2
import os
import pandas
from extraccion import extraerDatos
from dotenv import load_dotenv

load_dotenv ()

def transformarDatos (criptocurrency_data):
    df = pandas.DataFrame()
    # Todo: Definir Transformaciones
    return df


def cargarDatos (cryptocurrency_data):
    # Parámetros de conexión a Redshift
    dbname = os.getenv('NAME_DATABASE') 
    user = os.getenv('USER_DATABASE') 
    password = os.getenv('PASSWORD_DATABASE')
    host = os.getenv('HOST_DATABASE')  
    port = '5439'  

    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS cryptocurrency_data (
        id VARCHAR(50) NOT NULL,
        time TIMESTAMP NOT NULL,
        symbol VARCHAR(10) NOT NULL,
        name VARCHAR(100) NOT NULL,
        price FLOAT NOT NULL,
        circulating_supply FLOAT,
        date_added TIMESTAMP NOT NULL,
        PRIMARY KEY (id, time)
    )
    """)

    for data in cryptocurrency_data:
        cur.execute ("""
            INSERT INTO cryptocurrency_data (id, time, symbol, name, price, circulating_supply, date_added)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, data)

    conn.commit ()

    cur.close ()
    conn.close ()

