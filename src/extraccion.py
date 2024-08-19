import requests
import json
import pandas as pd
import psycopg2
import os
import datetime
from dotenv import load_dotenv

load_dotenv()

def extraerDatos ():
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
            time = datetime.datetime.now()
            name = crypto['name']
            symbol = crypto['symbol']
            price = crypto['quote']['USD']['price']
            circulating_supply = crypto.get('circulating_supply', None)
            date_added = crypto['date_added']

            cryptocurrency_data.append((id, time, symbol, name, price, circulating_supply, date_added))

        return cryptocurrency_data
    
    else:
        print("Error al obtener los datos de la API  de CoinMarketCap")
        return []