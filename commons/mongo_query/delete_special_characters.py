from pymongo import MongoClient
import sys
import os

# 경로 설정
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config_reader import read_config  # config read 용

def remove_symbols_from_mongo_symbols(ip_add, db_name, collection_name):
    # MongoDB에 연결
    client = MongoClient(ip_add)  # MongoDB URI
    db = client[db_name]
    collection = db[collection_name]

    # 특정 기호가 포함된 문서 삭제
    result = collection.delete_many({
        'symbol': {
            '$regex': '[.^/]',  # . 또는 ^ 또는 /가 포함된 경우
        }
    })

    print(f"Deleted {result.deleted_count} documents.")

if __name__ == '__main__':
    config = read_config()
    ip_add = config['MongoDB_remote']['ip_add']
    db_name = config['MongoDB_remote']['db_name']
    col_name = 'COL_AMERICA_CORPLIST'
    # 사용 예시
    remove_symbols_from_mongo_symbols(ip_add, db_name, col_name)