# main에서 다른 폴더 경로 참조하려면 필요
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 직접 만든 class    
from commons.config_reader import read_config # config read 용       
from commons.mongo_insert_recode import connect_mongo as connect_mongo_insert
from commons.mongo_find_recode import connect_mongo as connect_mongo_find

# mongo DB 동작
from pymongo import MongoClient
# yfinance
import yfinance as yf
# padas
import pandas as pd
# datetime
from datetime import datetime 
import pytz

class api_stockprice_yfinance:

    def get_stockprice_yfinance(symbol_list):
        return_histlist = pd.DataFrame()  # 빈 DataFrame 생성
        print(f"start:{datetime.now(pytz.timezone('Asia/Seoul'))}")
        for symbol in symbol_list:
            msft = yf.Ticker(symbol) # "MSFT"
            # get all stock info
            msft.info
            # get historical market data
            hist = msft.history(period="max")
            if hist is not None and not hist.empty:  # hist가 None이 아니고 비어있지 않은 경우
                hist['symbol'] = symbol  # 'symbol' 컬럼 추가
                # 인덱스를 'Date' 컬럼으로 변환
                df_with_date = hist.reset_index()
                # DataFrame을 레코드 리스트로 변환 후 머지
                return_histlist = pd.concat([return_histlist, df_with_date])
            print(f"loop:{datetime.now(pytz.timezone('Asia/Seoul'))}")
        print(hist)

        return return_histlist
    

def working_api_yfinance_stockpricing() :

    # 스케쥴러 등록 
    # mongodb 가져올 수 있도록
    ip_add = f'mongodb://192.168.0.91:27017/'
    db_name = f'DB_SGMN'
    col_name = f'COL_STOCKPRICE_WORK' # 데이터 읽을 collection

    # MongoDB 서버에 연결 
    client = MongoClient(ip_add)
    '''
    apple_records = connect_mongo_find.get_records_dataframe(client, db_name, col_name, {"symbol": "AAPL"})
    specific_records = connect_mongo_find.get_records_dataframe(client, db_name, col_name, {"symbol": "AAPL", "iswork": "fin"})
    '''
    symbols = connect_mongo_find.get_unfinished_ready_records(client, db_name, col_name)

    symbols = connect_mongo_find.get_records_dataframe(client, db_name, col_name)


    # symbol 컬럼만 리스트로 변환
    symbol_list = symbols['symbol'].tolist()
    result_list = api_stockprice_yfinance.get_stockprice_yfinance(symbol_list=symbol_list)

    # 히스토리 데이터 저장
    col_name = f'COL_STOCKPRICE_HISTORY'
    connect_mongo_insert.insert_recode_in_mongo(client, db_name, col_name, result_list)

    # 작업 상태 업데이트
    col_name = f'COL_STOCKPRICE_WORK'
    update_data_list = []
    for index, row in symbols.iterrows():
        update_data = {
            'ref_id': row['_id'],  # 원본 레코드의 ID를 참조 ID로 저장
            'iswork': 'fin',
            'symbol': row['symbol']
        }
        update_data_list.append(update_data)
    
    # 새로운 레코드로 삽입
    connect_mongo_insert.insert_recode_in_mongo(client, db_name, col_name, pd.DataFrame(update_data_list))

    pass




if __name__ == '__main__':

    corp_list = ['010950.ks']
    api_stockprice_yfinance.get_stockprice_yfinance(corp_list)

    # working_api_yfinance_stockpricing()

    pass
