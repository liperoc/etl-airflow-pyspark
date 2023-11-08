import requests
import json

currency_url = 'https://brapi.dev/api/v2/currency'

params = {
    'currency': 'USD-BRL,EUR-BRL,GBP-BRL,JPY-BRL,CNY-BRL,AED-BRL,CHF-BRL,ARS-BRL',
    'token': 'gSYkn4vunDe14h6j4QBHfgu',
}

response = requests.get(currency_url, params=params)

with open('/home/liperoc/projects/etl_finance_pyspark/json/currency.json', 'w') as outfile:
    json.dump(response.json(), outfile, ensure_ascii=False)
