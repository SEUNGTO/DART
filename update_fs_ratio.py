import numpy as np
import pandas as pd
import FinanceDataReader as fdr
from config import *
from sqlalchemy import Float, Numeric, String
from sqlalchemy.dialects.oracle import FLOAT as ORACLE_FLOAT
from sqlalchemy.dialects.oracle import BINARY_DOUBLE

price = fdr.StockListing('ETF/KR')[['Symbol', 'Name', 'Price']]
price.columns = ['etf_code', 'etf_name', 'price']

num_col = ['순이익률(%)', 'ROE(자기자본이익률, %)', 'ROA(총자산이익률, %)', 
        '부채비율(%)',  '유동비율(%)',
        'EPS',  'BPS', 'PER', 'PBR', 
        '매출채권회전율', '재고자산회전율', '매입채무회전율', 
        '매출채권회전일수', '재고자산회전일수', '매입채무회전일수']

date_order = 0
result = pd.DataFrame()
for code in price['etf_code'] :

    query = f"""
        SELECT *
        FROM fs_etf_table
        WHERE TO_CHAR(etf_code) = :code
            and date_order = :date_order
    """
    
    data = pd.read_sql(query, con=engine, params={'code': code, 'date_order' : date_order})
    if data.empty :
        continue
    data = data.pivot_table(index = 'account_name', columns = 'bas_date', values = 'amount')
    index = [
        '유동자산', '현금및현금성자산', '재고자산', '매출채권', '비유동자산', '자산총계',
        '유동부채', '매입채무', '비유동부채', '부채총계',
        '자본금', '이익잉여금', '자본총계',
        '매출액', '영업이익', '법인세차감전 순이익', '당기순이익(손실)',
    ]
    idx = [i for i in index if i in data.index]
    data = data.loc[idx]

    q = """SELECT "CU수량" FROM etf_info WHERE TO_CHAR("단축코드") = :code"""
    n_cu = pd.read_sql(q, con = engine, params = {'code' : code}) 
    n_cu = n_cu['CU수량'].values[0]
    
    sales = data.T.get('매출액', np.nan)
    net_profit = data.T.get('당기순이익(손실)', np.nan)
    
    current_asset = data.T.get('유동자산', np.nan)
    current_liabilty = data.T.get('유동부채', np.nan)

    liability = data.T.get('부채총계', np.nan)
    capital = data.T.get('자본총계', np.nan)
    asset = data.T.get('자산총계', np.nan)

    payable = data.T.get('매입채무', np.nan)
    receivable = data.T.get('매출채권', np.nan)
    inventory = data.T.get('재고자산', np.nan)

    ratio = pd.DataFrame()
    ratio['순이익률(%)'] = net_profit / sales * 100
    ratio['ROE(자기자본이익률, %)'] = net_profit / capital * 100
    ratio['ROA(총자산이익률, %)'] = net_profit / asset * 100
    ratio['부채비율(%)'] = liability / capital * 100
    ratio['유동비율(%)'] = current_asset / current_liabilty * 100

    ratio['EPS'] = net_profit / int(n_cu)
    ratio['BPS'] = capital / int(n_cu)

    p = price.loc[price['etf_code'] == code, 'price'].values[0]
    ratio['PER'] = p/(net_profit / int(n_cu))
    ratio['PBR'] = p/(capital / int(n_cu))

    ratio['매출채권회전율'] = sales/receivable
    ratio['재고자산회전율'] = sales/inventory
    ratio['매입채무회전율'] = sales/payable

    ratio['매출채권회전일수'] = 365/(sales/receivable)
    ratio['재고자산회전일수'] = 365/(sales/inventory)
    ratio['매입채무회전일수'] = 365/(sales/payable)
    
    ratio['현금순환주기(CCC)'] = ratio['재고자산회전일수'] + ratio['매출채권회전일수'] - ratio['매입채무회전일수']

    ratio = ratio.reset_index(drop= True)
    ratio['ETF코드'] = code
    
    result = pd.concat([result, ratio])
    

result[num_col] = result[num_col].apply(lambda x : round(x, 2))
result[num_col] = result[num_col].astype(float)
result['현금순환주기(CCC)'] = result['현금순환주기(CCC)'].apply(lambda x : round(x, 2))
result['현금순환주기(CCC)'] = result['현금순환주기(CCC)'].astype(float)
result['ETF코드'] = result['ETF코드'].astype(str)

import pdb
pdb.set_trace()

result.to_sql(
    'fs_etf_ratio', 
    con = engine, 
    if_exists='replace', 
    index = False,
    dtype = {
        '순이익률(%)' : Numeric(18, 6), 
        'ROE(자기자본이익률, %)' : Numeric(18, 6), 
        'ROA(총자산이익률, %)' : Numeric(18, 6), 
        '부채비율(%)' : Numeric(18, 6),  
        '유동비율(%)' : Numeric(18, 6),
        'EPS' : Numeric(18, 6),  
        'BPS' : Numeric(18, 6), 
        'PER' : Numeric(18, 6), 
        'PBR' : Numeric(18, 6), 
        '매출채권회전율' : Numeric(18, 6), 
        '재고자산회전율' : Numeric(18, 6), 
        '매입채무회전율' : Numeric(18, 6), 
        '매출채권회전일수' : Numeric(18, 6), 
        '재고자산회전일수' : Numeric(18, 6), 
        '매입채무회전일수' : Numeric(18, 6),
        '현금순환주기(CCC)' : Numeric(18, 6),
        'ETF코드' : String(6),
    },)


# result.to_sql(
#     'fs_etf_ratio', 
#     con = engine, 
#     if_exists='replace', 
#     index = False, 

#     )