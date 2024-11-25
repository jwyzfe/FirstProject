# main에서 다른 폴더 경로 참조하려면 필요
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# # 직접 만든 class    
# from commons.config_reader import read_config # config read 용       
# from commons.mongo_insert_recode import connect_mongo as connect_mongo_insert
# from commons.mongo_find_recode import connect_mongo as connect_mongo_find

# mongo DB 동작
from pymongo import MongoClient
# yfinance
import yfinance as yf
# padas
import pandas as pd
# datetime
from datetime import datetime, timedelta 

import pytz


def history_minute_data(ticker_symbol, start_date, end_date):
    """
    1분 간격의 데이터를 다운로드하는 함수입니다.
    
    :param tickers: str 또는 list
        다운로드할 티커 목록
    :param start_date: str
        다운로드 시작 날짜 (YYYY-MM-DD 형식)
    :param end_date: str
        다운로드 종료 날짜 (YYYY-MM-DD 형식)
    :return: DataFrame
        다운로드된 데이터가 포함된 DataFrame
    """
    # 필요한 라이브러리 임포트
    import pandas as pd

    # Ticker 객체 생성
    ticker = yf.Ticker(ticker_symbol)
    
    # history 메서드 호출
    data = ticker.history(
        interval='1m',  # 1분 간격
        start=start_date,
        end=end_date
        # actions=True,  # 배당금 및 주식 분할 데이터 포함
        # auto_adjust=True  # OHLC 자동 조정
    )
    
    return data


def download_minute_data(ticker_symbol, start_date, end_date):
    """
    1분 간격의 데이터를 다운로드하는 함수입니다.
    
    :param ticker_symbol: str
        다운로드할 티커
    :param start_date: str
        다운로드 시작 날짜 (YYYY-MM-DD 형식)
    :param end_date: str
        다운로드 종료 날짜 (YYYY-MM-DD 형식)
    :return: DataFrame
        다운로드된 데이터가 포함된 DataFrame
    """
    # yfinance의 download 메서드 사용
    data = yf.download(
        ticker_symbol,
        start=start_date,
        end=end_date,
        interval='1m'  # 1분 간격
    )
    
    return data

if __name__ == '__main__' :

    symbol = f'095570.KS'
    #msft = yf.Ticker(symbol) 

    # 사용 예시
    ticker_symbol = '005930.KS'  # 삼성전자 티커
    ticker_symbol = ["069500.KS", "252670.KS", "055550.KS", "010640.KS"] # 삼성전자 티커
    start_date = '2024-11-1'  # 시작 날짜 # 모든 종목 최초 시점 찾고 end 에서 부터 -2d 정도 빼게
    end_date = '2024-11-5'  # 종료 날짜 # datetime now로 가기 

    start_date = datetime.now() - timedelta(days=5)  # 2일 전
    end_date = datetime.now() + timedelta(days=1)    # 1일 후

    minute_data = download_minute_data(ticker_symbol, start_date, end_date)
    print(minute_data)

    # 약 20분 이전의 값이 나옴 
    # 10원 이하 절삭
    # 11.20 10시 5분 기준 11.18일 9시 부터 데이터 나옴 2일치가 최대 인듯
    # 인터벌로 돌아도 계속 밀릴듯
    # 이 버전은 좀 더 생각 해보기
    '''
    지금 까지 정리
    1. 사실 8일 씩 뒤로 돌리면 되는 거였음. 언제 까지 되돌아갈 수 있는지 확인 필요 => last 30 days.
    아마 이전에 테스트떄 장 마감일을 넣어서 테스트 한 거랑 invalid ticker로 테스트 해서 생긴 문제 였을 듯
    2. invalid ticker 있으면 데이터 출력이 잘 안되나? 이거 자세히 다시 봐야 할 지도? 
    개별로 하면 잘 되다가 묶어서 요청하면 잘 안나오는 경우가 있는 거 같음. 

    symbols = ["069500.KS", "252670.KS", "055550.KS", "010640.KS"]
    symbols = ["NVDA", "AAPL", "MSFT", "INVALID_SYMBOL", "AMZN", "META", "AVGO", "GOOGL", "TSLA", "GOOG", "BRK.B", '005930.KS', "010950.ks"]
    symbols = ["NVDA", "AAPL", "MSFT", "AMZN", "META", "AVGO", "GOOGL", "TSLA", "GOOG"]
    symbols = ["GOOG", "GOOGL", "TSLA"]

    '''