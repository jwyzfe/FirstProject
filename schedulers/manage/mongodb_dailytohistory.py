from typing import List, Dict, Set
from pymongo import MongoClient, ASCENDING
from pymongo.database import Database
from datetime import datetime
import pytz

# commons 폴더의 공용 insert 모듈 import
import sys
import os
# 현재 파일의 두 단계 상위 디렉토리(FirstProject)를 path에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))  # /manage
parent_dir = os.path.dirname(current_dir)  # /schedulers
project_dir = os.path.dirname(parent_dir)  # /FirstProject
sys.path.append(project_dir)
from commons.mongo_insert_recode import connect_mongo

class DataIntegrator:
    """데이터 통합 및 저장을 담당하는 클래스"""

    @staticmethod
    def process_all_daily_collections(daily_db: Database, resource_db: Database, 
                                    client: MongoClient, collection_config: Dict):
        """모든 일일 컬렉션 처리"""
        for job_type, config in collection_config.items():
            collections = config['collections']
            print(f"\nProcessing {job_type}: {collections['daily']} -> {collections['history']}")
            
            # 일일 데이터 조회 및 _id 필드 제거
            daily_data = list(daily_db[collections['daily']].find({}, {'_id': 0}))
            print(f"Found {len(daily_data)} documents in daily collection")
            
            if daily_data:
                # 데이터 삽입 전 중복 제거
                unique_data = DataIntegrator.remove_duplicates_from_list(
                    data_list=daily_data, 
                    duplicate_fields=config['integrator']['duplicate_fields']
                )
                print(f"Inserting {len(unique_data)} unique documents to resource collection")
                
                # 공용 insert 모듈 사용
                connect_mongo.insert_recode_in_mongo_notime(
                    client=client,
                    dbname=resource_db.name,
                    collectionname=collections['history'],
                    input_list=unique_data
                )

                # daily 컬렉션 삭제
                daily_db[collections['daily']].drop()
                print(f"Dropped daily collection: {collections['daily']}")

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
    TEST_PIPELINE_CONFIG = {
        'yfinance': {
            'collections': {
                'source': 'COL_NAS25_KOSPI25_CORPLIST',
                'work': 'COL_STOCKPRICE_DAILY_WORK',
                'daily': 'COL_STOCKPRICE_EMBEDDED_DAILY',
                'history': 'COL_STOCKPRICE_EMBEDDED'
            },
            'integrator': {
                'duplicate_fields': ['SYMBOL', 'TIME_DATA.DATE']
            }
        },
        'toss': {
            'collections': {
                'source': 'COL_NAS25_KOSPI25_CORPLIST',
                'work': 'COL_SCRAPPING_TOSS_COMMENT_DAILY_WORK',
                'daily': 'COL_SCRAPPING_TOSS_COMMENT_DAILY',
                'history': 'COL_SCRAPPING_TOSS_COMMENT_HISTORY'
            },
            'integrator': {
                'duplicate_fields': ['COMMENT', 'DATETIME']
            }
        },
        'stocktwits': {
            'collections': {
                'source': 'COL_NAS25_KOSPI25_CORPLIST',
                'work': 'COL_SCRAPPING_STOCKTWITS_COMMENT_DAILY_WORK',
                'daily': 'COL_SCRAPPING_STOCKTWITS_COMMENT_DAILY',
                'history': 'COL_SCRAPPING_STOCKTWITS_COMMENT_HISTORY'
            },
            'integrator': {
                'duplicate_fields': ['CONTENT', 'DATETIME']
            }
        },
        'yahoo': {
            'collections': {
                'source': '',
                'work': '',
                'daily': 'COL_SCRAPPING_NEWS_YAHOO_DAILY',
                'history': 'COL_SCRAPPING_NEWS_YAHOO_HISTORY'
            },
            'integrator': {
                'duplicate_fields': ['NEWS_URL']
            }
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
        collection_config=TEST_PIPELINE_CONFIG
    )