# main에서 다른 폴더 경로 참조하려면 필요
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 직접 만든 class    
from commons.config_reader import read_config # config read 용       
from commons.mongo_insert_recode import connect_mongo as connect_mongo_insert
from commons.mongo_find_recode import connect_mongo as connect_mongo_find

# mongo DB
from pymongo import MongoClient
    
def make_direct_insert_work_recode_news(classifications):
    work_records = []
    
    # classifications 리스트가 비어있지 않은 경우에만 처리
    if classifications:
        for classification in classifications:
            # 500부터 1까지 역순으로 반복
            for page_num in range(500, 0, -1):
                url = f'https://www.hankyung.com/{classification}?page={page_num}'    
                work_record = {
                    # 'classification': classification,  # 분류명
                    # 'page': page_num,  # 페이지 번호 (500 ~ 1)
                    'url' : url,
                    'iswork': 'ready'  # 초기 상태는 'ready'
                }
                work_records.append(work_record)
    
    return work_records

if __name__ == '__main__' :


    config = read_config()
    ip_add = f'mongodb://192.168.0.91:27017/'
    # MongoDB 연결 설정 
    # mongo_client = MongoClient(config['mongoDB']['ip'])
    mongo_client = MongoClient(ip_add)

    # 스케쥴러 등록 
    # ip_add = config['mongoDB']['ip']
    db_name = 'DB_SGMN'
    # col_name_work = 'COL_STOCKPRICE_WORK'
    col_name_work = 'COL_SCRAPPING_HANKYUNG_WORK'
    # col_name_find = 'COL_AMERICA_CORPLIST'

    page_list=['economy', 'financial-market', 'industry', 'politics', 'society', 'international']
   
    # ready 상태의 work 레코드 생성
    work_records = make_direct_insert_work_recode_news(page_list)

    # 생성된 레코드들을 COL_STOCKPRICE_WORK에 삽입
    if work_records:  # 레코드가 있는 경우에만 삽입
        connect_mongo_insert.insert_recode_in_mongo(
            mongo_client, 
            db_name, 
            col_name_work, 
            work_records
        )

