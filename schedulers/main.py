
# 스크래퍼 import 
from apscheduler.schedulers.background import BackgroundScheduler
# mongo DB 동작
from pymongo import MongoClient
# time.sleep
import time

# selenium driver 
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService

# main에서 다른 폴더 경로 참조하려면 필요
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 직접 만든 class          
from commons.mongo_insert_recode import connect_mongo
from commons.api_send_requester import ApiRequester
from commons.sel_iframe_courtauction import iframe_test
from commons.bs4_do_scrapping import bs4_scrapping

# 직접 구현한 부분을 import 해서 scheduler에 등록
from devs.api_test_class import api_test_class

# commons 로 옮길 예정
# insert collect 에도 네이밍 규칙 필요 
def register_job_with_mongo(client, ip_add, db_name, col_name, func, insert_data):
    
    result_data = func(*insert_data)

    try:
        if client is None:
            client = MongoClient(ip_add) # 관리 신경써야 함.
        result_list = connect_mongo.insert_recode_in_mongo(client, db_name, col_name, result_data)
        # print(f'insert id list count : {len(result_list.inserted_ids)}')
    except Exception as e :
        print(e)

    return 
'''
chrome_options = Options()
chrome_options.add_argument("--headless")  # Ensure GUI is off
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
'''

import logging
def main(message):

    # ip url tag 등등 최대한 전달 할 수 있도록 
    # log MongoDB 연결 설정
    mongo_client = MongoClient('mongodb://192.168.0.91:27017/')
    db = mongo_client['log_database']  # 사용할 데이터베이스 이름
    log_collection = db['logs']  # 사용할 컬렉션 이름

    # MongoDB에 로그 저장 함수
    def log_to_mongo(level, message):
        log_entry = {
            'level': level,
            'message': message,
            'timestamp': logging.Formatter('%(asctime)s').format(logging.LogRecord('', 0, '', 0, '', '', '', '')),
        }
        log_collection.insert_one(log_entry)

    # 로깅 설정
    class MongoDBHandler(logging.Handler):
        def emit(self, record):
            log_entry = self.format(record)
            log_to_mongo(record.levelname, log_entry)

    # 로깅 핸들러 추가
    mongo_handler = MongoDBHandler()
    mongo_handler.setLevel(logging.INFO)
    mongo_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    # 로거 설정
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(mongo_handler)

    '''
    레코드에 프로세서 얹제 시작 종료, 플래그 완료 여부, 에러 메세지, 아니면 완료, 
    insert id 기억 
    무조건 insert  => 그래도 id 기억 해야함. 두개의 레코드가 서로 연관 할 수 있게 
    구분 카테고리를 미리 생각해두어야 
    log는 주로 insert위주 인게 맞다 
    정제가 볼 수 있게 카테고리 이름을 미리 정해 두어야함. 
    시간이라 겹침 

    처음 단위랑 주기 단위 달라질 수 있음. 


    '''
    
    # 스케쥴러 등록 
    # mongodb 가져올 수 있도록
    ip_add = f'mongodb://192.168.0.91:27017/'
    db_name = f'lotte_db_sanghoonlee'
    col_name = f'lotte_col_sanghoonlee' # 안바꾸는게 나을지 

    # MongoDB 서버에 연결 : Both connect in case local and remote
    client = MongoClient(ip_add) # 관리 신경써야 함.

    scheduler = BackgroundScheduler()
    
    check_flag = [] # 리스트가 나으려나?

    url = f'http://underkg.co.kr/news'

    # 여기에 함수 등록 
    # 넘길 변수 없으면 # insert_data = []
    insert_data = [] # [val1,val2,val3]
    
    func_list = [
        {"func" : bs4_scrapping.do_scrapping, "args" : insert_data},
        {"func" : api_test_class.api_test_func,  "args" : insert_data}
    ]

    # working function 등록
    # id에 함수명 같은거 넣으면 나중에 어느 함수에서 문제 있었는지 알 수 있을 듯
    # 데일리 시간 단위 그래서 필요한 시간대 를 셋팅할 수 있게 
    # 특정 시간 몰릴 때 어떻게 할 수 있을 지 
    for func in func_list:
        scheduler.add_job(register_job_with_mongo, 
                        trigger='cron', # 데일리 특정시간 단위 
                        year='*',        # 매년
                        month='*',       # 매월
                        day='*',         # 매일
                        week='*',        # 매주
                        day_of_week='*', # 모든 요일
                        hour=12,         # 매일 12시에 실행
                        minute=0,        # 매일 12시 0분에 실행
                        second=0,        # 매일 12시 0분 0초에 실행
                        coalesce=True, 
                        max_instances=1,
                        id = '', # 특정 이름을 주어야 함? 
                        # args=[args_list]
                        args=[client, ip_add, db_name, col_name, func['func'], func['args']] # 
                        )

    # 등록된 함수들 상태 check function
    # 작업 상태 확인 함수
    def check_jobs(check_flag):
        jobs = scheduler.get_jobs()
        for job in jobs:
            check_flag.append(job) # 메모리 너무 쌓일 듯 잘 관리하던가 다른 방법 생각하기 
            logging.info(f"Job ID: {job.id}, Next Run Time: {job.next_run_time}, Trigger: {job.trigger}")
            pass
    
    # 상태 체크 작업 추가 (5초마다 실행)
    scheduler.add_job(check_jobs, 'interval', seconds=5, id='check_jobs_id', max_instances=1, coalesce=True, args=[check_flag])
    # 정제 function 등록

    # 스케쥴러 시작 및 에러데이터 로거 
    # 어떤 시도가 성공 했는지 
    # 어떤 시도가 실패 했는지
    # 왜 실패 했는지
    # 다른 행동에 영향을 주는지 
    # check_flag = set_flag() <= add_job 안에서 셋되어야 함.
    try:
        scheduler.start()
        #logger.info("스케줄러가 시작되었습니다.")
        pass
        # 메인 루프
        while True:
            pass
            if check_flag :
                pass

    except Exception as e:
        # logger.error(f"스케줄러에서 오류 발생: {e}")
        pass

    finally:
        scheduler.shutdown()
        # logger.info("스케줄러가 종료되었습니다.")
        pass
        return True


if __name__ == '__main__':
    main(f'task forever!')
