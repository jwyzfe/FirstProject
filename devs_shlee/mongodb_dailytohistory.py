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

class ResourceConsumer:
    """일일 작업 결과를 자원 DB에 저장하는 Consumer 클래스"""
    
    # # TimeSeries 인덱스가 필요한 컬렉션 정의
    # TIMESERIES_COLLECTIONS = {
    #     'COL_STOCKPRICE': ['SYMBOL', 'DATE']
    #     #'COL_TOSS_COMMENT': ['SYMBOL', 'CREATED_AT'],
    #     #'COL_STOCKTWITS_COMMENT': ['SYMBOL', 'CREATED_AT']
    # }

    # 중복 체크에 사용할 필드 정의 # 이거도 또 함수마다 달라 
    DUPLICATE_CHECK_FIELDS = {
        'COL_STOCKPRICE_HISTORY_TIME': ['SYMBOL', 'DATE'],
        'COL_SCRAPPING_TOSS_COMMENT_HISTORY': ['COMMENT', 'DATETIME'],
        'COL_SCRAPPING_STOCKTWITS_COMMENT_HISTORY': ['CONTENT', 'DATETIME'],
        'COL_YAHOOFINANCE_HISTORY': ['NEWS_URL']
    }

    # @staticmethod
    # def setup_collection_indexes(collection, collection_name: str):
    #     """컬렉션별 인덱스 설정"""
    #     if collection_name in ResourceConsumer.TIMESERIES_COLLECTIONS:
    #         index_fields = [(field, ASCENDING) for field in ResourceConsumer.TIMESERIES_COLLECTIONS[collection_name]]
    #         collection.create_index(index_fields, unique=True)

    @staticmethod
    def remove_duplicates(collection, collection_name: str):
        """컬렉션 내 중복 데이터 제거"""
        if collection_name in ResourceConsumer.DUPLICATE_CHECK_FIELDS:
            check_fields = ResourceConsumer.DUPLICATE_CHECK_FIELDS[collection_name]
            pipeline = [
                {"$group": {
                    "_id": {field: f"${field}" for field in check_fields},
                    "dups": {"$push": "$_id"},
                    "count": {"$sum": 1}
                }},
                {"$match": {
                    "count": {"$gt": 1}
                }}
            ]
            
            # 중복 데이터 찾기
            duplicates = list(collection.aggregate(pipeline))
            
            # 각 중복 그룹에서 가장 오래된 문서를 제외한 나머지 삭제
            for dup in duplicates:
                # 첫 번째 문서를 제외한 나머지 ID 목록
                dup_ids = dup["dups"][1:]
                collection.delete_many({"_id": {"$in": dup_ids}})
                print(f"Removed {len(dup_ids)} duplicate documents from {collection_name}")

    @staticmethod
    def process_daily_collection(daily_db: Database, resource_db: Database, 
                               daily_collection: str, resource_collection: str,
                               client: MongoClient):
        """일일 컬렉션 처리"""
        print(f"Processing {daily_collection} -> {resource_collection}")
        
        # # 인덱스 설정
        # ResourceConsumer.setup_collection_indexes(resource_db[resource_collection], resource_collection)
        
        # 일일 데이터 조회 및 _id 필드 제거
        daily_data = list(daily_db[daily_collection].find({}, {'_id': 0}))  # _id 필드 제외
        
        if daily_data:
            # 공용 insert 모듈 사용
            connect_mongo.insert_recode_in_mongo_notime(
                client=client,
                dbname=resource_db.name,
                collectionname=resource_collection,
                input_list=daily_data
            )
            
            # 중복 제거
            ResourceConsumer.remove_duplicates(
                resource_db[resource_collection],
                resource_collection
            )

    @staticmethod
    def process_all_daily_collections(daily_db: Database, resource_db: Database, 
                                    collection_mapping: Dict[str, str],
                                    client: MongoClient):
        """모든 일일 컬렉션 처리"""
        for daily_collection, resource_collection in collection_mapping.items():
            ResourceConsumer.process_daily_collection(
                daily_db, 
                resource_db, 
                daily_collection, 
                resource_collection,
                client
            )

if __name__ == "__main__":
    # 데이터베이스 연결
    client = MongoClient('mongodb://192.168.0.91:27017/')
    daily_db = client['DB_TEST']
    resource_db = client['DB_TEST']
    
    # 컬렉션 매핑 설정
    collection_mapping = {
        'COL_STOCKPRICE_DAILY': 'COL_STOCKPRICE_HISTORY_TIME',
        'COL_SCRAPPING_TOSS_COMMENT_DAILY': 'COL_SCRAPPING_TOSS_COMMENT_HISTORY',
        'COL_SCRAPPING_STOCKTWITS_COMMENT_DAILY': 'COL_SCRAPPING_STOCKTWITS_COMMENT_HISTORY',
        'COL_SCRAPPING_NEWS_YAHOO_DAILY': 'COL_SCRAPPING_NEWS_YAHOO_HISTORY'
        #'COL_SCRAPPING_HANKYUNG_DAILY': 'COL_HANKYUNG',
        #'COL_FINANCIAL_DAILY': 'COL_FINANCIAL'
    }
    
    ResourceConsumer.process_all_daily_collections(
        daily_db=daily_db,
        resource_db=resource_db,
        collection_mapping=collection_mapping,
        client=client
    )