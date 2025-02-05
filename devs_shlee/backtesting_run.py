
# 스크래퍼 import 
from apscheduler.schedulers.background import BackgroundScheduler
# mongo DB 동작
from pymongo import MongoClient
# time.sleep
import time
# 
import pandas as pd

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
from devs_shlee.release.api_stockprice_yfinance_daily import api_stockprice_yfinance 
from devs_shlee.release.sel_comment_scrap_stocktwits import comment_scrap_stocktwits 

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
            BATCH_SIZE = 7
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
                    'REF_ID': row['_id'],  # 원본 레코드의 ID를 참조 ID로 저장
                    'ISWORK': 'fin',
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
    ip_add = config['MongoDB_remote']['ip_add']
    db_name = config['MongoDB_remote']['db_name']

    # MongoDB 서버에 연결 
    client = MongoClient(ip_add) 

    scheduler = BackgroundScheduler()
  

    # 여기에 함수 등록 
    # 넘길 변수 없으면 # insert_data = []
    insert_data = "symbol" # [val1,val2,val3]
    
    '''
    func : 수행할 일 함수
    args : 그 일에 필요한 파라미터 => work 디비에 등록하면, 해당 컬럼만 리스트로 전달
    target : 최종 저장할 데이터 베이스 컬렉션 이름 
    work : 일 시킬 내용 들어있는 데이터 베이스 컬렉션 이름

    '''
    # 스케줄 설정을 위한 딕셔너리
    SCHEDULE_CONFIGS = {
        'news_5h': {
            'trigger': 'interval',
            'hours': 5,
        },
        'comment_1h': {
            'trigger': 'interval',
            'hours': 1,
        },
        'comment_30m': {
            'trigger': 'interval',
            'minutes': 30,
        }
    }

    func_list = [
        { 
            "func": api_stockprice_yfinance.get_stockprice_yfinance_daily, 
            "args": "symbol", 
            "target": 'COL_STOCKPRICE_DAILY', 
            "work": "COL_STOCKPRICE_DAILY_WORK",
            "schedule": "comment_30m"
        }
    ]

    for func in func_list:
        schedule_config = SCHEDULE_CONFIGS[func['schedule']]
        
        scheduler.add_job(
            register_job_with_mongo_cron,                         
            trigger=schedule_config['trigger'],
            coalesce=True, 
            max_instances=1,
            id=func['func'].__name__,
            args=[client, ip_add, db_name, func['work'], func['target'], func['func'], func['args']],
            **{k: v for k, v in schedule_config.items() if k != 'trigger'}
        )
        '''
        scheduler.add_job(
                    register_job_with_mongo_cron,                         
                    trigger='cron',  # 크론 트리거 사용
                    second='0',      # 매 분의 0초에 실행
                    minute='*',      # 매 분마다 실행
                    hour='*',        # 매 시간마다 실행
                    day='*',         # 매일 실행
                    month='*',       # 매월 실행
                    day_of_week='*', # 매주 실행
                    coalesce=True, 
                    max_instances=1,
                    id=func['func'].__name__,  # 독립적인 함수 이름 주어야 함.
                    args=[client, ip_add, db_name, func['work'], func['target'], func['func'], func['args']]
                )
        
        '''
    
    
    scheduler.start()

    while True:

        pass
    
    return True


if __name__ == '__main__':
    run()
    pass
