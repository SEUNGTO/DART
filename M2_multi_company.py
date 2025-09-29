import pandas as pd
import os
import requests
import pdb
import time

def prevent_ban(fn) : 
    def wait(*args, **kwargs) :
        a = time.time()
        result = fn(*args, **kwargs)
        b = time.time() - a
        time.sleep(max(60/1000 - b, 0))

        return result

    return wait

@prevent_ban
def fetch_multi_company_report(corp_code, year, report_code) : 
    url = 'https://opendart.fss.or.kr/api/fnlttMultiAcnt.json'
    params = {
        'crtfc_key' : os.getenv('DART_API_KEY'),
        'corp_code' : corp_code,
        'bsns_year' : year,
        'reprt_code' : report_code,
    }
    response = requests.get(url, params=params).json()
    
    if response['status'] == '000' :
        return pd.DataFrame(response['list'])

    else :
        return pd.DataFrame()
        
    


# 고유번호 불러오기
code_list = pd.read_csv('data/corp_code.csv', sep = "\t", dtype = {'corp_code' : str})
code_list.dropna(inplace=True)

# 스팩기업(SPAC) 제외
idx = code_list['corp_eng_name'].str.lower().str.contains('purpose|acquisition')
code_list = code_list[~idx]
code_list = code_list['corp_code'].to_list()

# 기본설정
batch_size = 30
iter = len(code_list) // batch_size + 1

reports = [
    (2024, '11013'),    # 2024년 1분기
    (2024, '11012'),    # 2024년 반기보고서
    (2024, '11014'),    # 2024년 3분기
    (2024, '11011'),    # 2024년 사업보고서
    (2025, '11013'),    # 2025년 1분기
    (2025, '11012'),    # 2025년 반기
]

for year, report_code in reports :
    
    print(year, report_code, "                              ")

    data = pd.DataFrame()
    for i in range(iter) :
        


        start = i * batch_size
        end = start + batch_size
        corp_code = ",".join(code_list[start:end])

        buffer = fetch_multi_company_report(corp_code, year, report_code)
        
        if not buffer.empty :            
            data = pd.concat([data, buffer])
            
        print(f"{(i+1)/iter * 100:.2f}% Done | Size : {data.shape}         ", end = "\r")

    data.to_csv(f'data/FS_{year}_{report_code}.csv', sep = "\t", index = False)