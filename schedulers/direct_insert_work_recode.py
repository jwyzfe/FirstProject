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

def make_direct_insert_work_recode(symbols_df) :

    work_records = []
    
    # symbols_df가 비어있지 않은 경우에만 처리
    if not symbols_df.empty:
        for _, row in symbols_df.iterrows():
            work_record = {
                'symbol': row['symbol'],  # symbol 컬럼의 값
                'iswork': 'ready' #symbol을 ready라고 세팅후 work symbol이랑 ready뜨면 work  //symbol 끝나면 insert 세팅 플래그 final로 바꾸고 
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
    col_name_work = 'COL_STOCKPRICE_WORK'
    col_name_find = 'COL_AMERICA_CORPLIST'

    # col_america_corplist에서 symbol 데이터 읽어오기
    symbols_df = connect_mongo_find.get_records_dataframe(
        mongo_client,
        db_name,
        col_name_find
    )
    
    # ready 상태의 work 레코드 생성
    work_records = make_direct_insert_work_recode(symbols_df)

    # 생성된 레코드들을 COL_STOCKPRICE_WORK에 삽입
    if work_records:  # 레코드가 있는 경우에만 삽입
        connect_mongo_insert.insert_recode_in_mongo(
            mongo_client, 
            db_name, 
            col_name_work, 
            work_records
        )

