import requests
import json
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# Parámetros de conexión a Redshift
dbname = os.getenv('NAME_DATABASE') 
user = os.getenv('USER_DATABASE') 
password = os.getenv('PASSWORD_DATABASE')
host = os.getenv('HOST_DATABASE')  
port = '5439'  

# Consumo de la API de CoinMarketCap
headers = {
    'X-CMC_PRO_API_KEY': os.getenv('X-CMC_PRO_API_KEY'),
    'Accepts': 'application/json',
}

params = {
    'start': '1',
    'limit': '100',
    'convert': 'USD'
}

url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
response = requests.get(url, params=params, headers=headers)

if response.status_code == 200:
    data = response.json()  
    cryptocurrencies = data['data']  

    cryptocurrency_data = []
    
    for crypto in cryptocurrencies:
        id = crypto['id']
        name = crypto['name']
        symbol = crypto['symbol']
        price = crypto['quote']['USD']['price']

        cryptocurrency_data.append((id, symbol, name, price))

    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS cryptocurrency_data (
        id VARCHAR(50),
        symbol VARCHAR(10),
        name VARCHAR(100),
        price FLOAT
    )
    """)

    cur.executemany("""
    INSERT INTO cryptocurrency_data (id, symbol, name, price)
    VALUES (%s, %s, %s, %s)
    """, cryptocurrency_data)

    conn.commit()

    cur.close()
    conn.close()
else:
    print("Error al obtener los datos de la API de CoinMarketCap")         
    