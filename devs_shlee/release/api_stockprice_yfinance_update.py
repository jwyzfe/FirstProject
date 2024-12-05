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
from datetime import datetime, timedelta 
import time
import pytz

class api_stockprice_yfinance:
   
    def get_stockprice_yfinance_period_down(symbol, start_time=None, end_time=None, interval=None):
        """단일 심볼에 대한 주가 데이터 다운로드"""
        try:
            # 데이터 다운로드
            data = yf.download(
                symbol,
                start=start_time,
                end=end_time,
                interval=interval,
                actions=True,  # 배당금 및 주식 분할 데이터 포함
            )

            # 데이터가 비어있지 않은 경우
            if not data.empty:
                # NaN 값이 있는지 확인
                if data.isnull().values.any():
                    print(f"Warning: Data for {symbol} contains NaN values. Skipping this symbol.")
                    return pd.DataFrame()

                # 멀티 인덱스 처리
                if isinstance(data.columns, pd.MultiIndex):
                    # 첫 번째 레벨의 인덱스 제거
                    data.columns = data.columns.get_level_values(0)

                # 데이터 포맷팅
                df_formatted = pd.DataFrame({
                    'date': data.index,
                    'open': data['Open'],
                    'high': data['High'],
                    'low': data['Low'],
                    'close': data['Close'],
                    'volume': data['Volume'],
                    'dividends': data['Dividends'],
                    'stocksplits': data['Stock Splits'],
                    'symbol': symbol
                })

                # 새로운 문서 구조로 변환
                transformed_data = []
                for _, row in df_formatted.iterrows():
                    time_data = {
                        "DATE": row['date'],
                        "PRICE_DATA": {
                            "OPEN": row['open'],
                            "HIGH": row['high'],
                            "LOW": row['low'],
                            "CLOSE": row['close'],
                            "VOLUME": row['volume'],
                            "STOCKSPLITS": row.get('stocksplits', 0),
                            "DIVIDENDS": row.get('dividends', 0)
                        }
                    }
                    
                    new_doc = {
                        "SYMBOL": row['symbol'],
                        "TIME_DATA": time_data,
                        # "CREATED_AT": datetime.utcnow()
                    }
                    transformed_data.append(new_doc)

                return pd.DataFrame(transformed_data)
            else:
                print(f"No data found for symbol {symbol} in the given time period.")
                return pd.DataFrame()

        except Exception as e:
            print(f"Error downloading data for {symbol}: {e}")
            return pd.DataFrame()

    def get_stockprice_yfinance_daily(symbol_list):
        """심볼 리스트의 각 심볼에 대해 일일 주가 데이터 수집"""
        # 현재 시간에서 2일 전과 1일 후로 설정
        start_time = datetime.now() - timedelta(days=2)
        end_time = datetime.now() + timedelta(days=1)
        interval = '1d'

        # 결과를 저장할 DataFrame 초기화
        df_combined = pd.DataFrame()

        # 각 심볼별로 개별 처리
        for symbol in symbol_list:
            try:
                print(f"Processing symbol: {symbol}")
                df_download = api_stockprice_yfinance.get_stockprice_yfinance_period_down(
                    symbol,
                    start_time=start_time,
                    end_time=end_time,
                    interval=interval
                )
                
                if not df_download.empty:
                    df_combined = pd.concat([df_combined, df_download], ignore_index=True)
                    print(f"Successfully processed symbol: {symbol}")
                else:
                    print(f"No data retrieved for symbol: {symbol}")
                
                # API 호출 간 짧은 딜레이 추가
                # time.sleep(0.5)
                
            except Exception as e:
                print(f"Error processing symbol {symbol}: {e}")
                continue

        return df_combined

def test_yfinance_func():
    config = read_config()
    ip_add = config['MongoDB_local_shlee']['ip_add']
    db_name = config['MongoDB_local_shlee']['db_name']
    col_name = f'COL_STOCKPRICE_WORK'

    # MongoDB 서버에 연결 
    client = MongoClient(ip_add)

    symbols = connect_mongo_find.get_records_dataframe(client, db_name, col_name)
    symbol_list = symbols['symbol'].tolist()

    result_list = api_stockprice_yfinance.get_stockprice_yfinance_daily(symbol_list)

    if not result_list.empty:
        # 히스토리 데이터 저장
        col_name = f'COL_STOCKPRICE_EMBEDDED_DAILY'
        connect_mongo_insert.insert_recode_in_mongo(client, db_name, col_name, result_list)

        # 작업 상태 업데이트
        col_name = f'COL_STOCKPRICE_WORK'
        update_data_list = []
        for index, row in symbols.iterrows():
            update_data = {
                'REF_ID': row['_id'],
                'ISWORK': 'fin',
                'SYMBOL': row['symbol']
            }
            update_data_list.append(update_data)
        
        connect_mongo_insert.insert_recode_in_mongo(client, db_name, col_name, pd.DataFrame(update_data_list))

if __name__ == '__main__':
    symbols = ["NVDA", "AAPL", "MSFT", "INVALID_SYMBOL", "AMZN", "META", "AVGO", 
              "GOOGL", "TSLA", "GOOG", "BRK.B", '005930.KS', "010950.ks"]
    df_testprint = api_stockprice_yfinance.get_stockprice_yfinance_daily(symbols)
    print(df_testprint)