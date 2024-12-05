from typing import List, Dict, Set
from pymongo import MongoClient, ASCENDING
from pymongo.database import Database
from datetime import datetime
import pytz

# commons 폴더의 공용 insert 모듈 import
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from commons.mongo_insert_recode import connect_mongo

class DataIntegrator:
    """데이터 통합 및 저장을 담당하는 클래스"""

    @staticmethod
    def process_all_daily_collections(daily_db: Database, resource_db: Database, 
                                    client: MongoClient, collection_config: Dict):
        """모든 일일 컬렉션 처리"""
        for job_type, config in collection_config.items():
            print(f"\nProcessing {job_type}: {config['source']} -> {config['target']}")
            
            # 일일 데이터 조회 및 _id 필드 제거
            daily_data = list(daily_db[config['source']].find({}, {'_id': 0}))
            print(f"Found {len(daily_data)} documents in daily collection")
            
            if daily_data:
                # 데이터 삽입 전 중복 제거
                unique_data = DataIntegrator.remove_duplicates_from_list(
                    data_list=daily_data, 
                    duplicate_fields=config.get('duplicate_fields', [])
                )
                print(f"Inserting {len(unique_data)} unique documents to resource collection")
                
                # 공용 insert 모듈 사용
                connect_mongo.insert_recode_in_mongo_notime(
                    client=client,
                    dbname=resource_db.name,
                    collectionname=config['target'],
                    input_list=unique_data
                )

                # daily 컬렉션 삭제
                daily_db[config['source']].drop()
                print(f"Dropped daily collection: {config['source']}")

    @staticmethod
    def remove_duplicates_from_list(data_list: List[Dict], duplicate_fields: List[str]) -> List[Dict]:
        """데이터 리스트에서 중복 제거"""
        if not duplicate_fields:
            return data_list
            
        unique_data = {}
        
        for item in data_list:
            # 중복 체크를 위한 키 생성
            key_parts = []
            for field in duplicate_fields:
                if "." in field:
                    parent, child = field.split(".")
                    value = item.get(parent, {}).get(child)
                else:
                    value = item.get(field)
                key_parts.append(str(value))
            
            unique_key = tuple(key_parts)
            
            # 중복이 아닌 경우에만 저장
            if unique_key not in unique_data:
                unique_data[unique_key] = item
        
        removed_count = len(data_list) - len(unique_data)
        if removed_count > 0:
            print(f"Removed {removed_count} duplicates from data")
            
        return list(unique_data.values())

if __name__ == "__main__":
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

    # 데이터베이스 연결
    client = MongoClient('mongodb://192.168.0.48:27017/')
    daily_db = client['DB_SGMN']
    resource_db = client['DB_SGMN']
    
    DataIntegrator.process_all_daily_collections(
        daily_db=daily_db,
        resource_db=resource_db,
        client=client,
        collection_config=COLLECTION_CONFIG
    )