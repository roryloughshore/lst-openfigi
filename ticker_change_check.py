import os
import pandas as pd
from pathlib import Path
import json
import urllib.request, urllib.parse
import platformdirs
import pprint

# Use platformdirs to configure a standard location for the API key
appname = "EOD_Scripts"
appauthor = "LSTech"
app_folder = platformdirs.user_data_dir(appname, appauthor)
api_openfigi_filename = 'openfigi_key.txt'
api_openfigi_filepath = Path(app_folder, api_openfigi_filename)

try:
    with open(api_openfigi_filepath, 'r') as file:
        api_openfigi_key = file.read().rstrip()
except:
    raise Exception("Error: User needs to place API key in {}".format(app_folder))


def figi_query(jobs):
    """
    Function to query OpenFIGI API
    :param jobs: list of dictionaries with the query parameters

    :return: list of dictionaries with FIGI data
    """

    print("OpenFIGI Query")
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



dataPathEquitiesAttributes = Path('/home/data/shared/eod/data/instrumentLists/equities-attributes.csv')

columnsList = ['eod_ticker', 'open_figi']

df_Current_Figi = pd.read_csv(dataPathEquitiesAttributes)[columnsList]
df_Current_Figi = df_Current_Figi.rename(columns={'eod_ticker': 'ticker'})


list_figi_job = []
list_figiresponse = []
jobcount = 0

for index, row in df_Current_Figi.iterrows():
    jobcount += 1
    job = {"idType": "ID_BB_GLOBAL", "idValue": row['open_figi']}
    list_figi_job.append(job)

    if jobcount == 100:
        list_figiresponse += figi_query(list_figi_job)
        list_figi_job.clear()
        jobcount = 0

list_figi_lookup = []
for figi, row in zip(list_figiresponse, df_Current_Figi.iterrows()):
    tickervalue = ''
    if 'data' in figi:
        tickervalue = figi['data'][0]['ticker']
        figivalue = figi['data'][0]['figi']

    list_figi_lookup.append([row[1]['open_figi'], row[1]['ticker'], figivalue, tickervalue])


list_changed_ticker = []
for instrument in list_figi_lookup:
    if instrument[1] != instrument[3].replace('/', '-') + '.US':
        list_changed_ticker.append(instrument)



pprint.pp(list_changed_ticker)
