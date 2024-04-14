import os
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import datetime
import timeit
import json
import urllib.request, urllib.parse
import platformdirs
import csv
import pprint

appname = "EOD_Scripts"
appauthor = "LSTech"
app_folder = platformdirs.user_data_dir(appname, appauthor)
api_openfigi_filename = 'openfigi_key.txt'
api_openfigi_filepath = Path(app_folder, api_openfigi_filename)

try:
    with open(api_openfigi_filepath, 'r') as file:
        api_openfigi_key = file.read().rstrip()
except:
    raise Exception("Error: User needs to place API key in {}".format(folder))

dataPathIndexConstituents = Path('indexconstituents.csv')
dataPathFIGILookup = Path('indexconstituents-figiLookup.csv')

def figi_query(jobs):
    handler = urllib.request.HTTPHandler()
    opener = urllib.request.build_opener(handler)
    openfigi_url = 'https://api.openfigi.com/v3/mapping'
    request = urllib.request.Request(openfigi_url, data=bytes(json.dumps(jobs), encoding='utf-8'))
    request.add_header('Content-Type', 'application/json')
    request.add_header('X-OPENFIGI-APIKEY', api_openfigi_key)
    request.get_method = lambda: 'POST'
    connection = opener.open(request)
    if connection.code != 200:
        raise Exception('Bad response code {}'.format(str(response.status_code)))

    return json.loads(connection.read().decode('utf-8'))


US_Indexes = ['NDX Index', 'SPX Index', 'RTY Index']
UK_Indexes = ['UKX Index']
HK_Indexes = ['HSI Index']

df_IndexConstituents = pd.read_csv(dataPathIndexConstituents, names=['index', 'year', 'month', 'ticker'])
df_IndexConstituents = df_IndexConstituents.query('ticker.notnull()')
df_IndexConstituents['base_ticker'] = df_IndexConstituents['ticker'].str.split(" ").apply(lambda x: x[0])

df_USIndexConstituents = df_IndexConstituents.query('index in @US_Indexes')
df_UKIndexConstituents = df_IndexConstituents.query('index in @UK_Indexes')
df_HKIndexConstituents = df_IndexConstituents.query('index in @HK_Indexes')


df_USTickers = df_USIndexConstituents['base_ticker'].drop_duplicates().sort_values()
df_UKTickers = df_UKIndexConstituents['base_ticker'].drop_duplicates().sort_values()
df_HKTickers = df_HKIndexConstituents['base_ticker'].drop_duplicates().sort_values()

list_figi_job = []
list_figiresponse = []
jobcount = 0
list_figi_lookup = []

for index, ticker in df_USTickers.items():
    jobcount += 1
    job = {"idType": "TICKER", "idValue": ticker, "exchCode": "US", "includeUnlistedEquities": True}
    list_figi_job.append(job)

    if jobcount == 100:
        print("OpenFIGI request")
        list_figiresponse += figi_query(list_figi_job)
        list_figi_job.clear()
        jobcount = 0

if jobcount > 0:
    print("OpenFIGI request")
    list_figiresponse += figi_query(list_figi_job)

jobcount = 0
list_figi_job.clear()


for figi, row in zip(list_figiresponse, df_USTickers.items()):
    figivalue = ''
    if 'data' in figi:
        figivalue = figi['data'][0]['figi']

    list_figi_lookup.append([row[1] + ".US", figivalue])




list_figiresponse.clear()
for index, ticker in df_UKTickers.items():
    jobcount += 1
    job = {"idType": "TICKER", "idValue": ticker, "exchCode": "LN", "includeUnlistedEquities": True}
    list_figi_job.append(job)

    if jobcount == 100:
        print("OpenFIGI request")
        list_figiresponse += figi_query(list_figi_job)
        list_figi_job.clear()
        jobcount = 0

if jobcount > 0:
    print("OpenFIGI request")
    list_figiresponse += figi_query(list_figi_job)

jobcount = 0
list_figi_job.clear()


for figi, row in zip(list_figiresponse, df_UKTickers.items()):
    figivalue = ''
    if 'data' in figi:
        figivalue = figi['data'][0]['figi']

    list_figi_lookup.append([row[1] + ".LN", figivalue])


list_figiresponse.clear()
for index, ticker in df_HKTickers.items():
    jobcount += 1
    job = {"idType": "TICKER", "idValue": ticker, "exchCode": "HK", "includeUnlistedEquities": True}
    list_figi_job.append(job)

    if jobcount == 100:
        print("OpenFIGI request")
        list_figiresponse += figi_query(list_figi_job)
        list_figi_job.clear()
        jobcount = 0

if jobcount > 0:
    print("OpenFIGI request")
    list_figiresponse += figi_query(list_figi_job)

jobcount = 0
list_figi_job.clear()



for figi, row in zip(list_figiresponse, df_HKTickers.items()):
    figivalue = ''
    if 'data' in figi:
        figivalue = figi['data'][0]['figi']

    list_figi_lookup.append([row[1] + ".HK", figivalue])



pd.DataFrame(list_figi_lookup, columns=['bb_ticker', 'open_figi']).to_csv(dataPathFIGILookup, index=False)


