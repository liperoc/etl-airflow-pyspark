import requests
import json
import time

primerate_url = "https://brapi.dev/api/v2/prime-rate" #SELIC

params = {
    'country': 'brazil',
    'start': '01/12/2020',
    'end': time.strftime("%d-%m-%Y"),
    'sortBy': 'date',
    'sortOrder': 'desc',
    'token': 'gSYkn4vunDe14h6j4QBHfgu',
}

response = requests.get(primerate_url, params=params)

with open('/home/liperoc/projects/etl_finance_pyspark/json/primerate.json', 'w') as outfile:
    json.dump(response.json(), outfile, ensure_ascii=False)