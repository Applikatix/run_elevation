import requests
import pandas as pd

# https://api.energidataservice.dk/dataset/ElectricityProdex5MinRealtime?offset=0&start=2020-01-01T00:00&end=2020-12-31T00:00&sort=Minutes5UTC%20DESC

def extract(year, limit=0):
    url = 'https://api.energidataservice.dk/dataset/ElectricityProdex5MinRealtime'
    params = dict(
        offset=0,
        start=f"{year}-01-01T00:00",
        end=f"{year+1}-01-01T00:00",
        sort="Minutes5UTC DESC",
        limit=limit)
    
    response = requests.get(url=url, params=params)
    if (code := response.status_code) != 200:
        print(code)
        print(response.text)
    df = pd.DataFrame(
        response
        .json()
        .get('records', []))
    return df

def load(dataframe: pd.DataFrame):
    dataframe.to_csv(fname := 'energi_data.csv')
    return fname

def transform(filename):
    import csv
    res = dict(DK1=0, DK2=0)
    with open(filename) as f:
        for row in csv.DictReader(f):
            res[row['PriceArea']] += float(row['ExchangeGermany'])
    return res

def run(year):
    print('getting data...')
    dataframe = extract(year)
    print(dataframe)
    print('saving...')
    filename = load(dataframe)
    print(filename)
    print('calculating export...')
    eksport = transform(filename)
    print(eksport)

run(2023)
