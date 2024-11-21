# main에서 다른 폴더 경로 참조하려면 필요
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 직접 만든 class    
from config_reader import read_config  # config read 용       
from mongo_insert_recode import connect_mongo as connect_mongo_insert
from mongo_find_recode import connect_mongo as connect_mongo_find

# mongo DB
from pymongo import MongoClient
import pandas as pd

def read_symbols_from_csv(file_path):
    try:
        # CSV 파일 읽기
        df = pd.read_csv(file_path, encoding='euc-kr')  # 인코딩 설정
        # 필요한 컬럼만 선택
        df.columns = ['symbol', 'corp_name']  # 컬럼 이름 변경
        return df
    except Exception as e:
        print(f"CSV 파일 읽기 오류: {str(e)}")
        return pd.DataFrame()  # 빈 DataFrame 반환

def make_direct_insert_work_recode(df):
    work_records = []
    
    for index, row in df.iterrows():
        symbol = str(row['symbol']).zfill(6)  # 6자리로 맞추기
        symbol_market = f"{symbol}.KS"  # .KS 붙이기
        corp_name = row['corp_name']
        print(corp_name)
        work_record = {
            'symbol': symbol,
            'symbol_market': symbol_market,
            'corp_name': corp_name,
            # 'iswork': 'ready'
        }
        work_records.append(work_record)
    
    return work_records
    

if __name__ == '__main__':


    config = read_config()
    ip_add = config['MongoDB_remote']['ip_add']
    db_name = config['MongoDB_remote']['db_name']
    mongo_client = MongoClient(ip_add)

    db_name = 'DB_SGMN'
    col_name_work = 'COL_KOSPI_CORPLIST'

    # CSV 파일 경로 설정
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    csv_file_path = os.path.join(project_root, 'kospi_finaldata.csv')

    try:
        # CSV 파일에서 심볼 읽기
        df = read_symbols_from_csv(csv_file_path)
        
        if not df.empty:
            # ready 상태의 work 레코드 생성
            work_records = make_direct_insert_work_recode(df)

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