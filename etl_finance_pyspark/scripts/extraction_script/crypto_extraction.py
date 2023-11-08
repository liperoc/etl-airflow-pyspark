import requests
import json

crypto_url = 'https://brapi.dev/api/v2/crypto'

params = {
    'coin': 'BTC,ETC,MATIC,XRP,LINK,BNB,SOL,ADA,LTC,SHIB,AAVE',
    'currency': 'BRL',
    'token': 'gSYkn4vunDe14h6j4QBHfgu',
}

response = requests.get(crypto_url, params=params)

with open('/home/liperoc/projects/etl_finance_pyspark/json/crypto.json', 'w') as outfile:
    json.dump(response.json(), outfile, ensure_ascii=False)
