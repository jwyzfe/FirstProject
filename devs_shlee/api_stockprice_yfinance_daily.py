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

    def get_stockprice_yfinance_history(symbol_list):
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
        # print(hist)

        return return_histlist
    
    def get_stockprice_yfinance_period_hist(symbol_list, start_time=None, end_time=None, interval=None ):
        return_histlist = pd.DataFrame()  # 빈 DataFrame 생성
        #print(f"start:{datetime.now(pytz.timezone('Asia/Seoul'))}")
        for symbol in symbol_list:
            try:
                ticker = yf.Ticker(symbol) # "MSFT"
                # get all stock info
                # ticker.info
                # get historical market data
                hist = ticker.history(
                        interval=interval,  # 1분 간격
                        start=start_time,
                        end=end_time
                )
                if hist is not None and not hist.empty:  # hist가 None이 아니고 비어있지 않은 경우
                    hist['symbol'] = symbol  # 'symbol' 컬럼 추가
                    # 인덱스를 'Date' 컬럼으로 변환
                    df_with_date = hist.reset_index()
                    # DataFrame을 레코드 리스트로 변환 후 머지
                    return_histlist = pd.concat([return_histlist, df_with_date])
                #print(f"loop:{datetime.now(pytz.timezone('Asia/Seoul'))}")
                # print(hist)
            except Exception as e :
                print(f"Error processing symbol {symbol}: {e}")
                continue

        return return_histlist
    
    '''
    def download(tickers, start=None, end=None, actions=False, threads=True,
        ignore_tz=None, group_by='column', auto_adjust=False, back_adjust=False,
        repair=False, keepna=False, progress=True, period="max", interval="1d",
        prepost=False, proxy=None, rounding=False, timeout=10, session=None,
        multi_level_index=True):
        
    '''
    def get_stockprice_yfinance_period_down(symbol_list, start_time=None, end_time=None, interval=None):
        try:
            # 데이터 다운로드
            data = yf.download(
                symbol_list,
                start=start_time,
                end=end_time,
                interval=interval,
                actions=True,  # 배당금 및 주식 분할 데이터 포함
                group_by='ticker'  # 티커별로 그룹화
            )

            # 데이터가 비어있지 않은 경우
            if not data.empty:
                result_df = pd.DataFrame()  # 최종 결과를 저장할 DataFrame

                # MultiIndex인 경우
                if isinstance(data.columns, pd.MultiIndex):
                    for symbol in symbol_list:
                        if symbol in data.columns.levels[0]:  # 해당 심볼의 데이터가 있는 경우
                            # NaN 값이 있는지 확인
                            if data[symbol].isnull().values.any():
                                print(f"Warning: Data for {symbol} contains NaN values. Skipping this symbol.")
                                continue  # NaN이 있는 경우 해당 심볼을 건너뜀

                            df_formatted = pd.DataFrame({
                                'date': data.index,
                                'open': data[symbol]['Open'],
                                'high': data[symbol]['High'],
                                'low': data[symbol]['Low'],
                                'close': data[symbol]['Close'],
                                'volume': data[symbol]['Volume'],
                                'dividends': data[symbol]['Dividends'],  # 배당금 합계
                                'stocksplits': data[symbol]['StockSplits'],  # 주식 분할 합계
                                'symbol': symbol
                            })
                            result_df = pd.concat([result_df, df_formatted], ignore_index=True)

                # 단일 심볼인 경우
                else:
                    # NaN 값이 있는지 확인
                    if data.isnull().values.any():
                        print(f"Warning: Data for {symbol_list[0]} contains NaN values. Skipping this symbol.")
                        return pd.DataFrame()  # NaN이 있는 경우 빈 DataFrame 반환

                    df_formatted = pd.DataFrame({
                        'date': data.index,
                        'open': data['Open'],
                        'high': data['High'],
                        'low': data['Low'],
                        'close': data['Close'],
                        'volume': data['Volume'],
                        'dividends': data['Dividends'],  # 배당금 합계
                        'stocksplits': data['StockSplits'],  # 주식 분할 합계
                        'symbol': symbol_list[0] if isinstance(symbol_list, list) else symbol_list
                    })
                    result_df = pd.concat([result_df, df_formatted], ignore_index=True)

                return result_df.reset_index(drop=True)
            else:
                print("No data found for the given symbols and time period.")
                return pd.DataFrame()

        except Exception as e:
            print(f"Error downloading data: {e}")
            return pd.DataFrame()

    def get_stockprice_yfinance_daily(symbol_list):
        # 예외처리 !!
        # 현재 시간에서 2일 전과 1일 후로 설정 # 파라미터로 받아야? 
        start_time = datetime.now() - timedelta(days=2)  # 2일 전
        end_time = datetime.now() + timedelta(days=1)    # 1일 후
        interval = '1d'  # 1일 간격

        # 결과를 저장할 DataFrame 초기화
        df_combined = pd.DataFrame()

        # 심볼 리스트가 8개 이상일 경우 5개씩 슬라이스하여 처리
        if len(symbol_list) >= 8:
            for i in range(0, len(symbol_list), 5):
                sliced_symbols = symbol_list[i:i + 5]  # 5개씩 슬라이스
                df_download = api_stockprice_yfinance.get_stockprice_yfinance_period_down(
                    sliced_symbols,
                    start_time=start_time,
                    end_time=end_time,
                    interval=interval
                )
                df_combined = pd.concat([df_combined, df_download], ignore_index=True)  # 결과 병합
        else:
            # 심볼 리스트가 8개 미만일 경우 전체 리스트로 처리
            df_combined = api_stockprice_yfinance.get_stockprice_yfinance_period_down(
                symbol_list,
                start_time=start_time,
                end_time=end_time,
                interval=interval
            )

        return df_combined
        

def small_test_yfinance_func():

    # symbols = ["NVDA", "AAPL", "MSFT", "AMZN", "META", "GOOGL", "TSLA", "GOOG", "BRK.B", "AVGO"]
    # 심볼 리스트 (유효하지 않은 심볼 포함)
    symbols = ['AAPL', 'MSFT', 'INVALID_SYMBOL', 'GOOGL']
    # 현재 시간에서 2일 전과 1일 후로 설정
    start_time = datetime.now() - timedelta(days=2)  # 2일 전
    end_time = datetime.now() + timedelta(days=1)    # 1일 후
    interval = f'1d' # None # f'1d' # f'1h' # f'1m' 1d default? 

    # get_stockprice_yfinance_period_hist 수행 시간 측정
    start_hist = time.time()  # 시작 시간 기록
    df_history = api_stockprice_yfinance.get_stockprice_yfinance_period_hist(
        symbols,
        start_time=start_time,
        end_time=end_time,
        interval=interval
    )
    end_hist = time.time()  # 종료 시간 기록
    hist_duration = end_hist - start_hist  # 수행 시간 계산

    # get_stockprice_yfinance_period_down 수행 시간 측정
    start_down = time.time()  # 시작 시간 기록
    df_download = api_stockprice_yfinance.get_stockprice_yfinance_period_down(
        symbols,
        start_time=start_time,
        end_time=end_time,
        interval=interval
    )
    end_down = time.time()  # 종료 시간 기록
    down_duration = end_down - start_down  # 수행 시간 계산

    # 결과 출력
    print(f"History Data Retrieval Time: {hist_duration:.2f} seconds")
    print(f"Download Data Retrieval Time: {down_duration:.2f} seconds")

    print(df_history)
    print(df_download)
    pass

def test_yfinance_func():

    config = read_config()
    ip_add = config['MongoDB_local_shlee']['ip_add']
    db_name = config['MongoDB_local_shlee']['db_name']
    col_name = f'COL_STOCKPRICE_WORK' # 데이터 읽을 collection

    # MongoDB 서버에 연결 
    client = MongoClient(ip_add)

    # symbols = connect_mongo_find.get_unfinished_ready_records(client, db_name, col_name)
    symbols = connect_mongo_find.get_records_dataframe(client, db_name, col_name)

    # symbol 컬럼만 리스트로 변환
    symbol_list = symbols['symbol'].tolist() # start time and end time , symbol, 

    result_list = api_stockprice_yfinance.get_stockprice_yfinance_history(symbol_list=symbol_list)

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

    # small_test_yfinance_func()
    # corp_list = ['010950.ks']
    # api_stockprice_yfinance.get_stockprice_yfinance(corp_list)
    # working_api_yfinance_stockpricing()

    symbols = ["NVDA", "AAPL", "MSFT", "INVALID_SYMBOL", "AMZN", "META", "AVGO", "GOOGL", "TSLA", "GOOG", "BRK.B", '005930.KS', "010950.ks"]
    # 심볼 리스트 (유효하지 않은 심볼 포함)
    # symbols = ['AAPL', 'MSFT', 'INVALID_SYMBOL', 'GOOGL']
    # symbols = ["NVDA"]
    df_testprint = api_stockprice_yfinance.get_stockprice_yfinance_daily(symbols)
    print(df_testprint)
    pass
