
# 스크래퍼 import 
from apscheduler.schedulers.background import BackgroundScheduler
# mongo DB 동작
from pymongo import MongoClient
# time.sleep
import time
# 
import pandas as pd

# selenium driver 
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService

# 직접 만든 class나 func을 참조하려면 꼭 필요 => main processor가 경로를 잘 몰라서 알려주어야함.
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                
from commons.config_reader import read_config # config read 용       
from commons.mongo_insert_recode import connect_mongo as connect_mongo_insert
from commons.api_send_requester import ApiRequester
from commons.templates.sel_iframe_courtauction import iframe_test
from commons.templates.bs4_do_scrapping import bs4_scrapping
from commons.mongo_find_recode import connect_mongo as connect_mongo_find

# 직접 구현한 부분을 import 해서 scheduler에 등록
from devs.api_test_class import api_test_class
from devs_shlee.api_stockprice_yfinance_daily import api_stockprice_yfinance 
# from devs_shlee.test_yahoo_scrap import yahoo_finance_scrap 
from devs_oz.MarketSenti_yf import calc_market_senti
from devs_oz.news_scrapping_yahoo_headless import yahoo_finance_scrap
from devs_jihunshim.bs4_news_hankyung import bs4_scrapping
from devs_jiho.dartApi import CompanyFinancials

def api_test_func():
    # api
    city_list = ['도쿄','괌','모나코']
    key_list = ['lat', 'lon']
    pub_key = '39fb7b1c6d4e11e7483aabcb737ce7b0'
    for city in city_list:
        base_url = f'https://api.openweathermap.org/geo/1.0/direct'
        
        params={}
        params['q'] = city
        params['appid'] = pub_key

        result_geo = ApiRequester.send_api(base_url, params, key_list)

        base_url = f'https://api.openweathermap.org/data/2.5/forecast'
        
        params_w = {}
        for geo in result_geo:
            for key in key_list:
                params_w[key] = geo[key]
        params_w['appid'] = pub_key
        result_cont = ApiRequester.send_api(base_url, params_w)

        print(result_cont)


# common 에 넣을 예정
def register_job_with_mongo(client, ip_add, db_name, col_name_work, col_name_dest, func, insert_data):

    try:
        if client is None:
            client = MongoClient(ip_add)
        symbols = connect_mongo_find.get_unfinished_ready_records(client, db_name, col_name_work)
          
        # symbols가 비어있는지 확인
        if symbols.empty:
            print("zero recode. skip schedule")
            return
        
        # 60개씩 제한 # 40 
        BATCH_SIZE = 20
        symbols_batch = symbols.head(BATCH_SIZE)  # 처음 60개만 선택
        
        # symbol 컬럼만 리스트로 변환 => 추후 더 조치 필요 
        symbol_list = symbols_batch[insert_data].tolist()

        # 선택된 symbol 처리
        result_data = func(symbol_list)
        if client is None:
            client = MongoClient(ip_add)
        result_list = connect_mongo_insert.insert_recode_in_mongo(client, db_name, col_name_dest, result_data)
    
        # 처리된 60개의 symbol에 대해서만 상태 업데이트
        update_data_list = []
        for _, row in symbols_batch.iterrows():
            update_data = {
                'ref_id': row['_id'],  # 원본 레코드의 ID를 참조 ID로 저장
                'iswork': 'fin',
                insert_data : row[insert_data]
            }
            update_data_list.append(update_data)

        if client is None:
            client = MongoClient(ip_add)
        result_list = connect_mongo_insert.insert_recode_in_mongo(client, db_name, col_name_work, update_data_list)

    except Exception as e:
        print(e)
        # client.close()

    return 

# common 에 넣을 예정
def register_job_with_mongo_cron(client, ip_add, db_name, col_name_work, col_name_dest, func, insert_data):
    try:
        if client is None:
            client = MongoClient(ip_add)

        # col_name_work가 None인지 확인
        if col_name_work == '':
            # col_name_work가 None인 경우
            result_data = func()  # func() 호출
            print("fin : func")
            result_list = connect_mongo_insert.insert_recode_in_mongo(client, db_name, col_name_dest, result_data)
            print("fin : insert data ")
        else:
            # col_name_work가 None이 아닌 경우
            symbols = connect_mongo_find.get_unfinished_ready_records(client, db_name, col_name_work)
            
            # symbols가 비어있는지 확인
            if symbols.empty:
                print("zero record. skip schedule")
                return
            
            # 20개씩 제한
            BATCH_SIZE = 20
            symbols_batch = symbols.head(BATCH_SIZE)  # 처음 20개만 선택
            
            # symbol 컬럼만 리스트로 변환
            symbol_list = symbols_batch[insert_data].tolist()

            # 선택된 symbol 처리
            result_data = func(symbol_list)
            print("fin : func")
            result_list = connect_mongo_insert.insert_recode_in_mongo(client, db_name, col_name_dest, result_data)
            print("fin : insert data ")
        
            # 처리된 20개의 symbol에 대해서만 상태 업데이트
            update_data_list = []
            for _, row in symbols_batch.iterrows():
                update_data = {
                    'ref_id': row['_id'],  # 원본 레코드의 ID를 참조 ID로 저장
                    'iswork': 'fin',
                    insert_data: row[insert_data]
                }
                update_data_list.append(update_data)

            result_list = connect_mongo_insert.insert_recode_in_mongo(client, db_name, col_name_work, update_data_list)

    except Exception as e:
        print(e)
        # client.close()  # 필요 시 클라이언트 닫기

    return


def run():

    config = read_config()
    ip_add = config['MongoDB_local_shlee']['ip_add']
    db_name = config['MongoDB_local_shlee']['db_name']
    col_name = f'COL_STOCKPRICE_WORK' # 데이터 읽을 collection

    # MongoDB 서버에 연결 
    client = MongoClient(ip_add) 

    scheduler = BackgroundScheduler()
    
    url = f'http://underkg.co.kr/news'

    # 여기에 함수 등록 
    # 넘길 변수 없으면 # insert_data = []
    insert_data = "symbol" # [val1,val2,val3]
    
    '''
    func : 수행할 일 함수
    args : 그 일에 필요한 파라미터 => work 디비에 등록하면, 해당 컬럼만 리스트로 전달
    target : 최종 저장할 데이터 베이스 컬렉션 이름 
    work : 일 시킬 내용 들어있는 데이터 베이스 컬렉션 이름

    '''
    func_list = [
        # {"func" : api_stockprice_yfinance.get_stockprice_yfinance_history, "args" : "symbol", "target" : f'COL_STOCKPRICE_HISTORY', "work" : f'COL_STOCKPRICE_WORK'},
        # {"func" : calc_market_senti.get_market_senti_list, "args" : "symbol", "target" : f'COL_MARKETSENTI_HISTORY', "work" : f'COL_MARKETSENTI_WORK'},
        # {"func" : bs4_scrapping.bs4_news_hankyung, "args" : "url", "target" : f'COL_SCRAPPING_HANKYUNG_HISTORY', "work" : f'COL_SCRAPPING_HANKYUNG_WORK'},
        # {"func" : CompanyFinancials.get_financial_statements, "args" : "corp_regist_num", "target" : f'COL_FINANCIAL_HISTORY', "work" : f'COL_FINANCIAL_WORK'}
        # # {"func" : api_test_class.api_test_func,  "args" : []}
        {"func" : api_stockprice_yfinance.get_stockprice_yfinance_daily, "args" : "symbol", "target" : f'COL_STOCKPRICE_DAILY', "work" : f'COL_STOCKPRICE_WORK_DAILY'},
        {"func" : yahoo_finance_scrap.scrape_news_schedule_version, "args" : "symbol", "target" : f'COL_SCRAPPING_YAHOO_DAILY', "work" : ""}

    ]

    for func in func_list:
        # scheduler.add_job(register_job_with_mongo,                         
        #                 trigger='interval',
        #                 seconds=15, # 5초 마다 반복  50
        #                 coalesce=True, 
        #                 max_instances=1,
        #                 id=func['func'].__name__, # 독립적인 함수 이름 주어야 함.
        #                 # args=[args_list]
        #                 args=[client, ip_add, db_name, func['work'], func['target'], func['func'], func['args']] # 
        #                 )
        scheduler.add_job(register_job_with_mongo_cron,                         
                    trigger='cron',  # 크론 트리거 사용
                    minute='*',      # 매 분마다 실행
                    second='0',      # 매 분의 0초에 실행
                    coalesce=True, 
                    max_instances=1,
                    id=func['func'].__name__,  # 독립적인 함수 이름 주어야 함.
                    args=[client, ip_add, db_name, func['work'], func['target'], func['func'], func['args']]
                    )
    
    
    scheduler.start()

    while True:

        pass
    
    return True


if __name__ == '__main__':
    run()
    pass
