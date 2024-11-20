# 스케쥴러 import 
from apscheduler.schedulers.background import BackgroundScheduler
# mongo DB
from pymongo import MongoClient
# time.sleep
import time
# logger
import logging

# selenium driver 
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService

# main에서 다른 폴더 경로 참조하려면 필요
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 직접 만든 class    
from commons.config_reader import read_config # config read 용       
from commons.mongo_insert_recode import connect_mongo
from commons.api_send_requester import ApiRequester
from commons.templates.sel_iframe_courtauction import iframe_test
from commons.templates.bs4_do_scrapping import bs4_scrapping

# 직접 구현한 부분을 import 해서 scheduler에 등록
from devs.api_test_class import api_test_class

# MongoDB에 로그 저장 함수
def log_to_mongo(log_collection, level, message):
    log_entry = {
        'level': level,
        'message': message,
        'timestamp': logging.Formatter('%(asctime)s').format(logging.LogRecord('', 0, '', 0, '', '', '', '')),
    }
    log_collection.insert_one(log_entry)

# 로깅 설정 함수
def setup_logging(mongo_client):
    db = mongo_client['log_database']  # 사용할 데이터베이스 이름
    log_collection = db['logs']  # 사용할 컬렉션 이름

    class MongoDBHandler(logging.Handler):
        def emit(self, record):
            log_entry = self.format(record)
            log_to_mongo(log_collection, record.levelname, log_entry)

    # 로깅 핸들러 추가
    mongo_handler = MongoDBHandler()
    mongo_handler.setLevel(logging.INFO)
    mongo_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    # 로거 설정
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(mongo_handler)

    return log_collection  # 로그 컬렉션 반환

def register_job_with_mongo(client, ip_add, db_name, col_name, func, insert_data):
    try:
        # 작업 시작 로그
        logging.info(f'Starting job: {func.__name__}')
        
        result_data = func(*insert_data)
        
        if client is None:
            client = MongoClient(ip_add)  # 관리 신경써야 함.
        
        result_list = connect_mongo.insert_recode_in_mongo(client, db_name, col_name, result_data)
        
        # 작업 종료 로그
        logging.info(f'Finished job: {func.__name__}') 
        
    except Exception as e:
        logging.error(f'Error in job {func.__name__}: {str(e)}')
        # client.close()

def main(message):
    config = read_config()
    
    # MongoDB 연결 설정
    mongo_client = MongoClient(config['mongoDB']['ip'])
    log_collection = setup_logging(mongo_client)  # 로깅 설정 및 로그 컬렉션 가져오기

    # 스케쥴러 등록 
    ip_add = config['mongoDB']['ip']
    db_name = 'lotte_db_sanghoonlee'
    col_name = 'lotte_col_sanghoonlee' 

    # MongoDB 서버에 연결
    client = MongoClient(ip_add)  # 관리 신경써야 함.

    scheduler = BackgroundScheduler()
    
    check_flag = []  # 리스트가 나으려나?

    url = 'http://underkg.co.kr/news'

    # 여기에 함수 등록 
    insert_data = [url]  # [val1,val2,val3]
    
    func_list = [
        {"func": bs4_scrapping.do_scrapping, "args": insert_data},
        {"func": api_test_class.api_test_func, "args": []}
    ]

    '''
    # 여기에 함수 등록 
    # 넘길 변수 없으면 # insert_data = []
    insert_data = [url] # [val1,val2,val3]
    
    func_list = [
        {"func" : bs4_scrapping.do_scrapping, "args" : insert_data},
        {"func" : api_test_class.api_test_func,  "args" : []}
    ]

    # working function 등록
    # id에 함수명 같은거 넣으면 나중에 어느 함수에서 문제 있었는지 알 수 있을 듯
    # 데일리 시간 단위 그래서 필요한 시간대 를 셋팅할 수 있게 
    # 특정 시간 몰릴 때 어떻게 할 수 있을 지 
    for func in func_list:
        scheduler.add_job(register_job_with_mongo, 
                        # trigger='cron', # 데일리 특정시간 단위 
                        # year='*',        # 매년
                        # month='*',       # 매월
                        # day='*',         # 매일
                        # week='*',        # 매주
                        # day_of_week='*', # 모든 요일
                        # hour=12,         # 매일 12시에 실행
                        # minute=0,        # 매일 12시 0분에 실행
                        # second=0,        # 매일 12시 0분 0초에 실행
                        trigger='interval',
                        seconds=5,
    
    '''
    # working function 등록
    # 데일리 시간 단위 그래서 필요한 시간대 를 셋팅할 수 있게 
    # 특정 시간 몰릴 때 어떻게 할 수 있을 지 
    for func in func_list:
        scheduler.add_job(register_job_with_mongo, 
                        # trigger='cron', # 데일리 특정시간 단위 
                        # year='*',        # 매년
                        # month='*',       # 매월
                        # day='*',         # 매일
                        # week='*',        # 매주
                        # day_of_week='*', # 모든 요일
                        # hour=12,         # 매일 12시에 실행
                        # minute=0,        # 매일 12시 0분에 실행
                        # second=0,        # 매일 12시 0분 0초에 실행
                        trigger='interval',
                        seconds=5,
                        coalesce=True, 
                        max_instances=1,
                        id=func['func'].__name__, # 독립적인 함수 이름 주어야 함.
                        # args=[args_list]
                        args=[client, ip_add, db_name, col_name, func['func'], func['args']] # 
                        )

    # 등록된 함수들 상태 check function
    # 작업 상태 확인 함수
    def check_jobs(check_flag):
        jobs = scheduler.get_jobs()
        for job in jobs:
            # check_flag.append(job) # 메모리 너무 쌓일 듯 잘 관리하던가 다른 방법 생각하기 
            logging.info(f"Job ID: {job.id}, Next Run Time: {job.next_run_time}, Trigger: {job.trigger}")
            pass

    # 상태 체크 작업 추가 (5초마다 실행)
    scheduler.add_job(check_jobs, 'interval', seconds=5, id='check_jobs_id', max_instances=1, coalesce=True, args=[check_flag])

    try:
        scheduler.start()
        while True:
            pass
    except Exception as e:
        pass
    finally:
        scheduler.shutdown()
        return True

if __name__ == '__main__':
    main('task forever!')