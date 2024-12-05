
# 스크래퍼 import 
from apscheduler.schedulers.background import BackgroundScheduler
# mongo DB 동작
from pymongo import MongoClient
# time.sleep
import time
# 
import pandas as pd

# # selenium driver 
# from selenium import webdriver
# from webdriver_manager.chrome import ChromeDriverManager
# from selenium.webdriver.chrome.service import Service as ChromeService

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
from devs_shlee.release.api_stockprice_yfinance_update import api_stockprice_yfinance 
from devs_shlee.release.sel_comment_scrap_stocktwits import comment_scrap_stocktwits 
from devs_oz.release.MarketSenti_yf import calc_market_senti
from devs_oz.release.news_scrapping_yahoo_headless import yahoo_finance_scrap
from devs_jihunshim.release.bs4_news_hankyung import bs4_scrapping
from devs_jiho.release.dartApi import CompanyFinancials
from devs_jiho.release.ongoing_updateDailyTossComments import scrap_toss_comment
from manage.mongodb_producer import JobProducer
from manage.mongodb_queuemanager import QueueManager
from manage.mongodb_dailytohistory import DataIntegrator


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
                'REF_ID': row['_id'],  # 원본 레코드의 ID를 참조 ID로 저장
                'ISWORK': 'fin',
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


def update_job_status(client, db_name, col_name_work, symbols_batch, status, insert_data):
    """
    처리된 symbol에 대한 상태 업데이트를 수행하는 함수

    :param client: MongoDB 클라이언트
    :param db_name: 데이터베이스 이름
    :param col_name_work: 작업 컬렉션 이름
    :param symbols_batch: 상태를 업데이트할 symbol의 배치
    :param status: 업데이트할 상태 ('fin' 또는 'working')
    :param insert_data: 업데이트할 추가 데이터 필드
    """
    update_data_list = []
    for _, row in symbols_batch.iterrows():
        update_data = {
            'REF_ID': row['_id'],  # 원본 레코드의 ID를 참조 ID로 저장
            'ISWORK': status,  # 상태 업데이트
            insert_data: row[insert_data]
        }
        update_data_list.append(update_data)

    # 배치 업데이트
    result_list = connect_mongo_insert.insert_recode_in_mongo(client, db_name, col_name_work, update_data_list)
    return result_list


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
            
            ## 파라미터 처리 해야 되는 부분 
            # 20개씩 제한
            BATCH_SIZE = 1
            symbols_batch = symbols.head(BATCH_SIZE)  # 처음 20개만 선택
            
            # symbol 컬럼만 리스트로 변환
            symbol_list = symbols_batch[insert_data].tolist()

            # result_list = update_job_status(client, db_name, col_name_work, symbols_batch, 'working', insert_data)

            # 선택된 symbol 처리
            result_data = func(symbol_list)

            print("fin : func")
            result_list = connect_mongo_insert.insert_recode_in_mongo(client, db_name, col_name_dest, result_data)
            print("fin : insert data ")
        
            result_list = update_job_status(client, db_name, col_name_work, symbols_batch, 'fin', insert_data)
            
    except Exception as e:
        print(e)
        # client.close()  # 필요 시 클라이언트 닫기

    return


def run():

    config = read_config()
    ip_add = config['MongoDB_remote_readonly']['ip_add']
    db_name = config['MongoDB_remote_readonly']['db_name']
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
    '''
    함수별 필요 파라미터를 생각해보자
    symbol 인데 어떤 심볼인지 market 코드 붙은 건지 안붙은 건지 고를 수 있어야 함.
    batch 사이즈도 달라져야함. 각 스케쥴 마다 소요시간이 다르기 때문
    input db랑 target db를 알아야함.
    non 파라미터 경우 ? 
    시간을 받아서 처리해야 하는 경우도 있음. 
    '''
    # 추후 로거 필요하다면 wrapping 해야함
    # 작업 유형별 설정
    JOBS_CONFIG = {
        'yfinance': {
            'collection': 'COL_STOCKPRICE_DAILY_WORK',
            'source': {
                'collection': 'COL_NAS25_KOSPI25_CORPLIST',
                'symbol_field': 'SYMBOL'
            },
            'batch_size': 20
        },
        'toss': {
            'collection': 'COL_SCRAPPING_TOSS_COMMENT_DAILY_WORK',
            'source': {
                'collection': 'COL_NAS25_KOSPI25_CORPLIST',
                'symbol_field': lambda corp: corp['SYMBOL_KS'] if corp['MARKET'] == 'kospi' else corp['SYMBOL']
            },
            'batch_size': 5
        },
        'stocktwits': {
            'collection': 'COL_SCRAPPING_STOCKTWITS_COMMENT_DAILY_WORK',
            'source': {
                'collection': 'COL_NAS25_KOSPI25_CORPLIST',
                'symbol_field': 'SYMBOL',
                'filter': lambda corp: corp['MARKET'] == 'nasdaq'
            },
            'batch_size': 1
        },
        'yahoofinance': {
            'collection': 'COL_YAHOOFINANCE_DAILY_WORK',
            'count': 10,
            'batch_size': 1
        },
        'hankyung': {
            'collection': 'COL_SCRAPPING_HANKYUNG_DAILY_WORK',
            'url_base': 'https://www.hankyung.com/{category}?page={page}',
            'categories': ['economy', 'financial-market', 'industry', 
                         'politics', 'society', 'international'],
            'batch_size': 500
        },
        'financial': {
            'collection': 'COL_FINANCIAL_DAILY_WORK',
            'source': {
                'collection': 'COL_FINANCIAL_CORPLIST',
                'code_field': 'CORP_REGIST_NUM'
            },
            'batch_size': 10
        }
    }

    # 스케줄 설정을 위한 딕셔너리
    SCHEDULE_CONFIGS = {
        'hours_8': {
            'trigger': 'interval',
            'hours': 8,
        },
        'hours_3': {
            'trigger': 'interval',
            'hours': 3,
        },
        'minutes_10': {
            'trigger': 'interval',
            'minutes': 10,
        },
        'test_10': {
            'trigger': 'interval',
            'seconds': 10,
        }
    }

    # client_source = MongoClient(ip_add)
    # source_db = client_source['DB_SGMN']

    # client_target = MongoClient(ip_add)
    # target_db = client_target['DB_SGMN']

    # JobProducer.register_all_daily_jobs(source_db,target_db,client_target,JOBS_CONFIG)

    # scheduler.add_job(JobProducer.register_all_daily_jobs, 
    #                   **SCHEDULE_CONFIGS['test_10'],
    #                   id='register_all_daily_jobs', 
    #                   max_instances=1, 
    #                   coalesce=True, 
    #                   args=[source_db,target_db,client_target,JOBS_CONFIG]
    # )


    # client_man = MongoClient(ip_add)
    # db = client_man['DB_SGMN']
    # days = 1
    # scheduler.add_job(QueueManager.cleanup_work_collections, 
    #                   **SCHEDULE_CONFIGS['test_10'],
    #                   id='cleanup_work_collections', 
    #                   max_instances=1, 
    #                   coalesce=True, 
    #                   args=[db, days]
    #                   )
    
    client_con = MongoClient(ip_add)
    daily_db = client_con['DB_SGMN']
    resource_db = client_con['DB_SGMN']
        
    # 통합된 컬렉션 설정
    COLLECTION_CONFIG = {
        'yfinance': {
            'source': 'COL_STOCKPRICE_EMBEDDED_DAILY',
            'target': 'COL_STOCKPRICE_EMBEDDED',
            'duplicate_fields': ['SYMBOL', 'TIME_DATA.DATE']
        },
        'toss': {
            'source': 'COL_SCRAPPING_TOSS_COMMENT_DAILY',
            'target': 'COL_SCRAPPING_TOSS_COMMENT_HISTORY',
            'duplicate_fields': ['COMMENT', 'DATETIME']
        },
        'stocktwits': {
            'source': 'COL_SCRAPPING_STOCKTWITS_COMMENT_DAILY',
            'target': 'COL_SCRAPPING_STOCKTWITS_COMMENT_HISTORY',
            'duplicate_fields': ['CONTENT', 'DATETIME']
        },
        'yahoo': {
            'source': 'COL_SCRAPPING_NEWS_YAHOO_DAILY',
            'target': 'COL_SCRAPPING_NEWS_YAHOO_HISTORY',
            'duplicate_fields': ['NEWS_URL']
        }
    }

    scheduler.add_job(DataIntegrator.process_all_daily_collections, 
                      **SCHEDULE_CONFIGS['test_10'],
                      id='process_all_daily_collections', 
                      max_instances=1, 
                      coalesce=True, 
                      args=[daily_db, resource_db, client_con, COLLECTION_CONFIG]
                      )

    # func_list = [
    #     { 
    #         "func": api_stockprice_yfinance.get_stockprice_yfinance_daily, 
    #         "args": "SYMBOL", 
    #         "target": 'COL_STOCKPRICE_DAILY', 
    #         "work": "COL_STOCKPRICE_DAILY_WORK",
    #         "schedule": "test_10"
    #     },
    #     {
    #         "func": comment_scrap_stocktwits.run_stocktwits_scrap_list, 
    #         "args": "SYMBOL", 
    #         "target": 'COL_SCRAPPING_STOCKTWITS_COMMENT_DAILY', 
    #         "work": "COL_SCRAPPING_STOCKTWITS_COMMENT_DAILY_WORK",
    #         "schedule": "test_10"
    #     },
    #     {
    #         "func": yahoo_finance_scrap.scrape_news_schedule_version, 
    #         "args": "SYMBOL", 
    #         "target": 'COL_SCRAPPING_NEWS_YAHOO_DAILY', 
    #         "work": "",
    #         "schedule": "test_10"
    #     },
    #     {
    #         "func": scrap_toss_comment.run_toss_comments, 
    #         "args": "SYMBOL", 
    #         "target": 'COL_SCRAPPING_TOSS_COMMENT_DAILY', 
    #         "work": "COL_SCRAPPING_TOSS_COMMENT_DAILY_WORK",
    #         "schedule": "test_10"
    #     },
    #     {
    #         "func": bs4_scrapping.bs4_news_hankyung, 
    #         "args": "URL", 
    #         "target": 'COL_SCRAPPING_HANKYUNG_DAILY', 
    #         "work": "COL_SCRAPPING_HANKYUNG_DAILY_WORK",
    #         "schedule": "test_10"
    #     }
    # ]

    # for func in func_list:
    #     schedule_config = SCHEDULE_CONFIGS[func['schedule']]
        
    #     scheduler.add_job(
    #         register_job_with_mongo_cron,                         
    #         trigger=schedule_config['trigger'],
    #         coalesce=True, 
    #         max_instances=1,
    #         id=func['func'].__name__,
    #         args=[client, ip_add, db_name, func['work'], func['target'], func['func'], func['args']],
    #         **{k: v for k, v in schedule_config.items() if k != 'trigger'}
    #     )
    #     '''
    #     scheduler.add_job(
    #                 register_job_with_mongo_cron,                         
    #                 trigger='cron',  # 크론 트리거 사용
    #                 second='0',      # 매 분의 0초에 실행
    #                 minute='*',      # 매 분마다 실행
    #                 hour='*',        # 매 시간마다 실행
    #                 day='*',         # 매일 실행
    #                 month='*',       # 매월 실행
    #                 day_of_week='*', # 매주 실행
    #                 coalesce=True, 
    #                 max_instances=1,
    #                 id=func['func'].__name__,  # 독립적인 함수 이름 주어야 함.
    #                 args=[client, ip_add, db_name, func['work'], func['target'], func['func'], func['args']]
    #             )
        
    #     '''
    
    
    scheduler.start()

    while True:

        pass
    
    return True


if __name__ == '__main__':
    run()
    pass
