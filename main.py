#%%
import pandas as pd
import os
import pdb
import zipfile
import re

#%%
# 1. 압축해제
file_name_list = os.listdir('zip')
file_name = file_name_list[0]

for file in file_name_list : 
    with zipfile.ZipFile(f'zip/{file}', 'r', metadata_encoding="cp949") as zip_ref:
        zip_ref.extractall('extracted')

#%%
# 2. 추출된 파일 확인
extracted_files = os.listdir('extracted')

finance = ['은행', '증권', '보험', '금융']
normal_bs = [
    f for f in extracted_files 
    if not any(term in f for term in finance) and "재무상태표" in f
    ]

finance_bs = [
    f for f in extracted_files 
    if any(term in f for term in finance) and "재무상태표" in f
    ]

# %%
file_nm = normal_bs[0]
df = pd.read_csv(f'extracted/{file_nm}', encoding='cp949', sep = "\t")

# %%

file_nm = normal_bs[0]
data = pd.read_csv(f'extracted/{file_nm}', encoding='cp949', sep = "\t")

# 매입채무 추출 로직

result = pd.DataFrame()
# 1. 계정명이 정확히 매입채무인 경우 추출




# 1. 코드대로 보고하는 기업 추출
payables_code = [
    'ifrs-full_TradeAndOtherCurrentPayables',
    'dart_ShortTermTradePayables'    
]
include_code_idx = data['항목코드'].str.contains('|'.join(payables_code))
df = data[include_code_idx]


# 1-1. 코드 중 하나만 있는 경우 : 그대로 사용
grp_by = df.groupby('종목코드').count()['항목코드']
idx = grp_by[grp_by == 1].index
tmp = df[df['종목코드'].isin(idx)]
result = pd.concat([result, tmp])

# 1-2. 코드 둘 다 있는 경우
idx = grp_by[grp_by > 1].index
df2 = df[df['종목코드'].isin(idx)]

# 1-2-1. 계정명이 정확히 매입채무인 경우 사용
idx = df2['항목명'] == '매입채무'
tmp = df2[idx]
result = pd.concat([result, tmp])

# 1-2-2. 계정명이 정확히 매입채무가 아닌 경우, 계정코드가 'dart_ShortTermTradePayables'인 경우 사용
idx = df2['항목명'] != '매입채무'
tmp = df2[idx]
tmp = tmp[tmp['항목코드'] == 'dart_ShortTermTradePayables']
result = pd.concat([result, tmp])

# %%
# 2. 코드대로 보고하지 않는 기업 추출
df = data[~include_code_idx]

#%%
# 2-1. 계정명이 정확히 매입채무인 경우 사용
idx = df['항목명'].str.strip() == '매입채무'
tmp = df[idx]
result = pd.concat([result, tmp])

#%%
# 2-2. dart_LongTermTradeAndOtherNonCurrentPayables 또는 계정명에 “장기”가 포함된 경우 제외
con1 = df['항목명'] != '매입채무'
con2 = ~df['항목코드'].str.contains('dart_LongTermTradeAndOtherNonCurrentPayables')
con3 = ~df['항목명'].str.contains('장기')
idx = con1 | con2 | con3
tmp = df[~idx]



#%%
result.groupby('종목코드').count().sort_values('항목코드', ascending=False)
# %%
