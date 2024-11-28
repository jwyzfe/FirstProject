# main에서 다른 폴더 경로 참조하려면 필요
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 직접 만든 class    
# from commons.config_reader import read_config # config read 용       
from mongo_insert_recode import connect_mongo as connect_mongo_insert
from mongo_find_recode import connect_mongo as connect_mongo_find

# mongo DB
from pymongo import MongoClient
import json

def read_company_list(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        company_data = json.load(file)
        return company_data[0]  # JSON 파일이 리스트로 감싸져 있으므로 첫 번째 요소 반환


def make_direct_insert_work_recode(company_dict):
    work_records = []
    
    for company_name, corp_num in company_dict.items():
        # 빈 문자열이 아닌 경우에만 처리
        if corp_num:
            # 하이픈 제거
            corp_num_clean = corp_num.replace('-', '')
            
            work_record = {
                'CORP_REGIST_NUM': corp_num_clean, # corp_regist_num
                'COMPANY_NAME': company_name,  # 회사명도 저장
                # 'iswork': 'ready'
            }
            work_records.append(work_record)
    
    return work_records
    

if __name__ == '__main__':
    # config = read_config()
    ip_add = f'mongodb://192.168.0.50:27017/'
    mongo_client = MongoClient(ip_add)

    db_name = 'DB_SGMN'
    col_name_work = 'COL_FINANCIAL_CORPLIST'

    # JSON 파일 경로 설정
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    json_file_path = os.path.join(project_root, 'companyList.json')

    try:
        # JSON 파일 읽기
        company_data = read_company_list(json_file_path)
        
        # ready 상태의 work 레코드 생성
        work_records = make_direct_insert_work_recode(company_data)

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
        
    except Exception as e:
        print(f"오류 발생: {str(e)}")
    finally:
        mongo_client.close()

