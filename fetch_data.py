
import requests
import pdb
import pandas as pd
import os
import time
from tqdm import tqdm

def prevent_ban(fn) : 
    def wait(*args, **kwargs) :
        a = time.time()
        result = fn(*args, **kwargs)
        b = time.time() - a
        time.sleep(max(60/1000 - b, 0))

        return result

    return wait


@prevent_ban
def fetch_multiple_corp_data(corp_codes, year, report_code):
    url = 'https://opendart.fss.or.kr/api/fnlttMultiAcnt.json'
    params = {
        'crtfc_key' : os.getenv('DART_API_KEY'),
        'corp_code' : corp_codes,
        'bsns_year' : year,
        'reprt_code' : report_code,
    }
    response = requests.get(url, params=params)
    item = response.json()

    if item['status'] != '000':
        return pd.DataFrame()  # Return empty DataFrame on error
    else :
        return pd.DataFrame(item['list'])



corp_code = pd.read_csv('data/corp_code.csv', dtype={'corp_code': str}, sep = '\t')
corp_code = corp_code.dropna().reset_index(drop=True)

batch = 30
iter = len(corp_code) // batch + 1

year = '2024'
report_code = '11012'

reports = [
    (2023, '11014'),  # 2023년 3분기보고서
    (2023, '11011'),  # 2023년 사업보고서
    (2024, '11013'),  # 2024년 1분기보고서
    (2024, '11012'),  # 2024년 반기보고서
    (2024, '11014'),  # 2024년 3분기보고서
    (2024, '11011'),  # 2024년 사업보고서
    (2025, '11013'),  # 2025년 1분기보고서
    (2025, '11012'),  # 2025년 반기보고서
]

for year, report_code in reports:
    data = pd.DataFrame()
    print(f'Fetching data for year: {year}, report_code: {report_code}')

    for i in tqdm(range(iter), desc=f'Processing {year}-{report_code}'):
        start = i * batch
        end = start + batch
        batch_corp_code = corp_code.iloc[start:end]['corp_code'].tolist()
        corp_codes = ','.join(batch_corp_code)
        buffer = fetch_multiple_corp_data(corp_codes, year, report_code)
        data = pd.concat([data, buffer], ignore_index=True)
    
    os.makedirs('data', exist_ok=True)
    data.to_csv(f'data/financial_statements_{year}_{report_code}.csv', index=False)

pdb.set_trace()