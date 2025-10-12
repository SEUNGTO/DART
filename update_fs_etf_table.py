
import pandas as pd
import numpy as np
from tqdm import tqdm
from config import *
from sqlalchemy import Float
from sqlalchemy.dialects.oracle import FLOAT as ORACLE_FLOAT


def aggregate_quarter_to_year(date_order, engine) :
    
    q = """
    SELECT *
    FROM fs_base_table 
    WHERE date_order = :date_order 
      and TO_CHAR(fs_type) = 'BS'
     """
    BS_DATA = pd.read_sql(q, con = engine, params = {'date_order' : str(date_order)})   
   
    # 기준일자 추출
    date = BS_DATA['bas_date'].mode().values[0]    
    BS_DATA = BS_DATA[['stock_code', 'account_name', 'amount', 'share', 'amount_per_share']]

    IS_DATA = pd.DataFrame()

    for increment in range(4) :
        q = """
        SELECT *
        FROM fs_base_table
        WHERE date_order = :date_order 
          and TO_CHAR(fs_type) = 'IS'
        """
        buffer = pd.read_sql(q, con = engine, params = {'date_order' : date_order})
        IS_DATA = pd.concat([IS_DATA, buffer])
        date_order += increment

    cols = ['stock_code', 'account_name', 'amount', 'share', 'amount_per_share']
    IS_DATA = IS_DATA[cols]
    grp = IS_DATA.groupby(['stock_code', 'account_name'])
    amount = grp[['amount']].sum().reset_index()
      
    data = pd.concat([BS_DATA, amount])
    data['share'] = data.groupby('stock_code')['share'].transform(lambda x : x.ffill())
    data['amount_per_share'] = data['amount'] / data['share']
    data['bas_date'] = date
    
    return data.reset_index(drop = True)

if __name__ == '__main__' :

    etf_table = pd.read_sql('select * from etf_base_table', con = engine)
 
    fs_etf_table = pd.DataFrame()

    for date_order in range(4) :
        
        print(date_order)

        base_fs = aggregate_quarter_to_year(date_order, engine)
        
        for etf_code in tqdm(etf_table['etf_code'].unique()) :
            
            etf_deposit = etf_table[etf_table['etf_code'] == etf_code].copy()
            
            tmp = base_fs[base_fs['stock_code'].isin(etf_deposit['stock_code'])]
            tmp = tmp.set_index('stock_code').join(etf_deposit[['stock_code', 'recent_quantity']].set_index('stock_code'))
            tmp = tmp.dropna()
            tmp['amount_per_share'] = tmp['amount_per_share'] * tmp['recent_quantity']
            tmp['amount_per_share'] = tmp['amount_per_share'].replace([np.inf, -np.inf], np.nan)
            tmp = tmp.dropna()

            buffer = tmp.groupby('account_name')['amount_per_share'].sum().reset_index()
            buffer['etf_code'] = etf_code   # etf_code 추가
            buffer['date_order'] = date_order # date_order 추가
            buffer = buffer.join(tmp.reset_index()['bas_date']) # date 추가
            buffer['amount_per_share'] = pd.to_numeric(buffer['amount_per_share'], errors='coerce')
            buffer['amount_per_share'] = round(buffer['amount_per_share'], 0) # amount_per_share 추가

            buffer = buffer.rename(columns = {'amount_per_share' : 'amount'})
            fs_etf_table = pd.concat([fs_etf_table, buffer])
            
   
    fs_etf_table.to_sql(
        'fs_etf_table',
        con = engine,
        if_exists='replace', 
        index = False,
        dtype = {
            'amount' : Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
        })