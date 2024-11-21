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
        # print(hist)

        return return_histlist
    

'''
 "095570",
    "006840",
    "282330",
    "027410",
    "138930",
    "001460",
    "001040",
    "011150",
    "000590",
    "012030",
    "016610",
    "005830",
    "000990",
    "139130",
    "001530",
    "000210",
    "375500",
    "155660",
    "069730",
    "017860",
    "017940",
    "365550",
    "383220",
    "007700",
    "006360",
    "078930",
    "012630",
    "294870",
    "097230",
    "014790",
    "204320",
    "060980",
    "035000",
    "003560",
    "175330",
    "234080",
    "001060",
    "096760",
    "105560",
    "432320",
    "009070",
    "003620",
    "016380",
    "001390",
    "033180",
    "001940",
    "025000",
    "092230",
    "000040",
    "093050",
    "034220",
    "003550",
    "051900",
    "373220",
    "032640",
    "011070",
    "066570",
    "051910",
    "079550",
    "010120",
    "000680",
    "006260",
    "229640",
    "108320",
    "001120",
    "023150",
    "035420",
    "181710",
    "338100",
    "034310",
    "008260",
    "004250",
    "456040",
    "010950",
    "005090",
    "001380",
    "001770",
    "002360",
    "009160",
    "123700",
    "025530",
    "011790",
    "018670",
    "001740",
    "210980",
    "395400",
    "034730",
    "402340",
    "361610",
    "100090",
    "096770",
    "001510",
    "285130",
    "017670",
    "003570",
    "064960",
    "100840",
    "036530",
    "005610",
    "465770",
    "011810",
    "077970",
    "084870",
    "002710",
    "024070",
    "000500",
    "000860",
    "035250",
    "011420",
    "002100",
    "009450",
    "267290",
    "012320",
    "000050",
    "214390",
    "012610",
    "009140",
    "013580",
    "012200",
    "002140",
    "010130",
    "002240",
    "009290",
    "017040",
    "017900",
    "037710",
    "030610",
    "339770",
    "007690",
    "001140",
    "002720",
    "114090",
    "083420",
    "014530",
    "014280",
    "008870",
    "001570",

'''

def download_minute_data(ticker_symbol, start_date, end_date):
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

if __name__ == '__main__' :

    symbol = f'095570.KS'
    #msft = yf.Ticker(symbol) 

    # 사용 예시
    ticker_symbol = '005930.KS'  # 삼성전자 티커
    start_date = '2024-11-16'  # 시작 날짜
    end_date = '2024-11-21'  # 종료 날짜

    minute_data = download_minute_data(ticker_symbol, start_date, end_date)
    print(minute_data)

    # 약 20분 이전의 값이 나옴 
    # 10원 이하 절삭
    # 11.20 10시 5분 기준 11.18일 9시 부터 데이터 나옴 2일치가 최대 인듯
    # 인터벌로 돌아도 계속 밀릴듯
    # 이 버전은 좀 더 생각 해보기
