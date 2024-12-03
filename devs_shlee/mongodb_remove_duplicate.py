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

    # 중복 체크에 사용할 필드 정의 # 이거도 또 함수마다 달라 
    DUPLICATE_CHECK_FIELDS = {
        'COL_STOCKPRICE_EMBEDDED': ['SYMBOL', 'TIME_DATA.DATE'],  # 점 표기법으로 중첩 필드 접근
        'COL_SCRAPPING_TOSS_COMMENT_HISTORY': ['COMMENT', 'DATETIME'],
        'COL_SCRAPPING_STOCKTWITS_COMMENT_HISTORY': ['CONTENT', 'DATETIME'],
        'COL_YAHOOFINANCE_HISTORY': ['NEWS_URL']
    }

    @staticmethod
    def remove_duplicates(collection, collection_name: str):
        """컬렉션 내 중복 데이터 제거"""
        if collection_name in ResourceConsumer.DUPLICATE_CHECK_FIELDS:
            check_fields = ResourceConsumer.DUPLICATE_CHECK_FIELDS[collection_name]
            # 각 필드에 대한 참조 방식 결정
            field_refs = {}
            group_keys = {}
            for field in check_fields:
                if "." in field:
                    parent, child = field.split(".")
                    field_refs[field] = {"$getField": {"field": child, "input": f"${parent}"}}
                    group_keys[f"{parent}_{child}"] = field_refs[field]  # 점을 언더스코어로 대체
                else:
                    field_refs[field] = f"${field}"
                    group_keys[field] = field_refs[field]
            
            pipeline = [
                {"$group": {
                    "_id": group_keys,  # 수정된 키 이름 사용
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
                
                # # 삭제할 문서들의 정보 조회
                # docs_to_delete = collection.find({"_id": {"$in": dup_ids}})
                
                # print(f"\nDuplicate group found in {collection_name}:")
                # print(f"Group key values: {dup['_id']}")
                # print("Documents to be deleted:")
                # for doc in docs_to_delete:
                #     if collection_name == 'COL_STOCKPRICE_EMBEDDED':
                #         print(f"ID: {doc['_id']}, Symbol: {doc.get('SYMBOL')}, Date: {doc.get('time_data', {}).get('DATE')}")
                #     else:
                #         print(f"ID: {doc['_id']}, Content: {doc.get('CONTENT', doc.get('COMMENT', 'N/A'))}, DateTime: {doc.get('DATETIME')}")
                
                # 문서 삭제
                collection.delete_many({"_id": {"$in": dup_ids}})
                print(f"Removed {len(dup_ids)} duplicate documents\n")


if __name__ == "__main__":
    # 데이터베이스 연결
    client = MongoClient('mongodb://192.168.0.50:27017/')
    resource_db = client['DB_SGMN']
    
    # 아무리 그래도 너무 오래 걸림 자체 중복만 해결해서 넘기고 전체 중복은 아무래도 따로 하게 해야 할듯 
    # 컬렉션 매핑 설정
    collection_mapping = {
        'COL_STOCKPRICE_EMBEDDED_DAILY': 'COL_STOCKPRICE_EMBEDDED', # 이거는 최신을 넣는 거니까 캐싱하는게 나을 듯  
        # 'COL_SCRAPPING_TOSS_COMMENT_DAILY': 'COL_SCRAPPING_TOSS_COMMENT_HISTORY',
        # 'COL_SCRAPPING_STOCKTWITS_COMMENT_DAILY': 'COL_SCRAPPING_STOCKTWITS_COMMENT_HISTORY',
        # 'COL_SCRAPPING_NEWS_YAHOO_DAILY': 'COL_SCRAPPING_NEWS_YAHOO_HISTORY'
        #'COL_SCRAPPING_HANKYUNG_DAILY': 'COL_HANKYUNG',
        #'COL_FINANCIAL_DAILY': 'COL_FINANCIAL'
    }
    
    ResourceConsumer.remove_duplicates(resource_db["COL_STOCKPRICE_EMBEDDED"], "COL_STOCKPRICE_EMBEDDED")