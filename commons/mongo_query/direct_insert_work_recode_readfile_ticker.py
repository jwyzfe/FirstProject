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
import json
import pandas as pd

def read_symbols_from_csv(file_path):
    try:
        # CSV 파일 읽기 (Symbol 컬럼만)
        df = pd.read_csv(file_path, usecols=['Symbol'])
        # Symbol 컬럼을 리스트로 변환
        symbols = df['Symbol'].tolist()
        return symbols
    except Exception as e:
        print(f"CSV 파일 읽기 오류: {str(e)}")
        return []

def make_direct_insert_work_recode(symbols):
    work_records = []
    
    for symbol in symbols:
        if symbol:  # symbol이 비어있지 않은 경우만 처리
            work_record = {
                'symbol': symbol,
                'iswork': 'ready'
            }
            work_records.append(work_record)
    
    return work_records
    

if __name__ == '__main__':
    config = read_config()
    ip_add = f'mongodb://192.168.0.48:27017/'
    mongo_client = MongoClient(ip_add)

    db_name = 'DB_SGMN'
    col_name_work = 'COL_AMERICA_CORPLIST'

    # CSV 파일 경로 설정
    current_dir = os.path.dirname(os.path.abspath(__file__))
    csv_file_path = os.path.join(current_dir, 'nasdaq.csv')

    try:
        # CSV 파일에서 심볼 읽기
        symbols = read_symbols_from_csv(csv_file_path)
        
        if symbols:
            # ready 상태의 work 레코드 생성
            work_records = make_direct_insert_work_recode(symbols)

            print(f"총 {len(work_records)}개의 레코드가 생성되었습니다.")

            # 생성된 레코드들을 MongoDB에 삽입
            if work_records:
                connect_mongo_insert.insert_recode_in_mongo(
                    mongo_client, 
                    db_name, 
                    col_name_work, 
                    work_records
                )
                print("MongoDB에 데이터 삽입 완료")
        else:
            print("심볼을 찾을 수 없습니다.")
        
    except Exception as e:
        print(f"오류 발생: {str(e)}")
    finally:
        mongo_client.close()

