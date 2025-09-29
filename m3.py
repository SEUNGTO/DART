# 데이터 전처리 하기

import os
import pandas as pd
import pdb
import numpy as np
import re


def convert_to_number(col : pd.Series) -> list :
    
    if col.dtype == "O" :

        sign = np.where(col.str.contains('-'), -1, 1)

        col = col.apply(lambda x : re.sub(r"\D+", '', x))
        col = col.apply(lambda x : float(x) if x != "" else 0)
        col = col * sign
    
        return col
    

file_list = os.listdir('data')
file_list = [f for f in file_list if "FS" in f]

account_name = [
    '자산총계', '유동자산', '비유동자산',
    '부채총계', '유동부채', '비유동부채',
    '자본총계', '자본금', '이익잉여금',
    '매출액', '영업이익', '법인세차감전 순이익', '당기순이익', '당기순이익(손실)'
     
]

for file in file_list[-1:] :

    data = pd.read_csv(f"data/{file}", sep = "\t")
    buffer = data[data['account_nm'].isin(account_name)]
    buffer.loc[:, 'thstrm_amount'] = convert_to_number(buffer['thstrm_amount']).copy()

    pdb.set_trace()
    
    
    
