
# 스크래퍼 import 
from apscheduler.schedulers.background import BackgroundScheduler
# mongo DB 동작
from pymongo import MongoClient
# time.sleep
import time
from datetime import datetime, timedelta  

# selenium driver 
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService

# 직접 만든 class 
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                
from commons.mongo_insert_recode import connect_mongo
from commons.api_send_requester import ApiRequester
from commons.templates.sel_iframe_courtauction import iframe_test
from commons.templates.bs4_do_scrapping import bs4_scrapping

# 직접 구현한 부분을 import 해서 scheduler에 등록
from devs_shlee.api_stockprice_yfinance import api_stockprice_yfinance


# common 에 넣을 예정
def register_job_with_mongo(client, ip_add, db_name, col_name, func, insert_data):

    try:
        result_data = func(*insert_data)
        if client is None:
            client = MongoClient(ip_add) # 관리 신경써야 함.
        result_list = connect_mongo.insert_recode_in_mongo(client, db_name, col_name, result_data)       # print(f'insert id list count : {len(result_list.inserted_ids)}')
    except Exception as e :
        print(e)
        client.close()

    return 

def chunk_list(lst, n):
    """리스트를 n 크기로 나누는 제너레이터 함수"""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
        
def run():

    # 스케쥴러 등록 
    # mongodb 가져올 수 있도록
    ip_add = f'mongodb://localhost:27017/'
    db_name = f'DB_SGMN' # db name 바꾸기
    col_name = f'collection_name' # collection name 바꾸기

    # MongoDB 서버에 연결 
    client = MongoClient(ip_add) 

    scheduler = BackgroundScheduler()
    
    url = f'http://underkg.co.kr/news'

    # 여기에 함수 등록 
    # 넘길 변수 없으면 # insert_data = []
    # insert_data = [symbols] # [val1,val2,val3]
    
    # func_list = [
    #     # {"func" : bs4_scrapping.do_scrapping, "args" : insert_data},
    #     {"func" : api_stockprice_yfinance.api_test_func,  "args" : insert_data}
    # ]

    # 심볼 리스트를 50개씩 나누기
    batch_size = 60  # 한 번에 처리할 심볼 수 약 48초?
    symbol_batches = list(chunk_list(symbols, batch_size))

    # 각 배치별로 func_list 생성
    func_list = []
    for symbol_batch in symbol_batches:
        func_list.append({
            "func": api_stockprice_yfinance.api_test_func,
            "args": [symbol_batch]  # 각 배치를 리스트로 감싸서 전달
        })

    # 각 func에 대해 스케줄 등록
    for index, func in enumerate(func_list):
        # 각 배치마다 시작 시간을 조금씩 다르게 설정 (30초 간격)
        start_date = datetime.now() + timedelta(seconds=30 * index)
        
        scheduler.add_job(
            register_job_with_mongo,
            trigger='interval',
            seconds=180,  # 3분마다 반복
            start_date=start_date,  # 시작 시간을 다르게 설정
            coalesce=True,
            max_instances=1,
            id=f"{func['func'].__name__}_{index}",  # 기존 ID 형식 유지
            args=[client, ip_add, db_name, col_name, func['func'], func['args']]  # 기존 args 구조 유지
        )
        print(f"Scheduled {func['func'].__name__} batch {index} with {len(func['args'][0])} symbols, starting at {start_date}")

    
    scheduler.start()

    while True:

        pass
    
    return True


if __name__ == '__main__':
    run()
    pass
