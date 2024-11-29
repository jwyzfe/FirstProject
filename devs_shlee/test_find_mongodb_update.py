from pymongo import MongoClient
from datetime import datetime

def migrate_to_timeseries(source_collection, target_collection):
    total_migrated = 0  # 총 마이그레이션된 문서 수

    # 전체 문서 조회
    cursor = source_collection.find()
    
    for doc in cursor:
        # _id와 CREATED_AT 필드 제외
        doc.pop('_id', None)
        # doc.pop('CREATED_AT', None)  # 필요없는 경우
        
        # 필드 이름 변경
        if 'STOCK SPLITS' in doc:
            doc['STOCKSPLITS'] = doc.pop('STOCK SPLITS')  # 필드 이름 변경
        
        # 문서 삽입
        target_collection.insert_one(doc)
        total_migrated += 1
        
        # 진행 상황 출력
        if total_migrated % 1000 == 0:  # 1000개마다 출력
            print(f"Migrated {total_migrated} documents.")

    print(f"Migration completed. Total documents migrated: {total_migrated}")

if __name__ == "__main__":
    # MongoDB 클라이언트 연결
    client = MongoClient('mongodb://192.168.0.50:27017/')
    db = client['DB_SGMN']  # 데이터베이스 이름
   
    # Time Series 컬렉션 생성
    db.create_collection(
        "COL_STOCKPRICE_HISTORY_TIME",
        timeseries = {
            "timeField": "DATE",        # 시간 필드
            "metaField": "SYMBOL",      # 메타데이터 필드 (파티셔닝 기준)
            "granularity": "minutes"     # 시간 단위 (minutes가 가장 작은 단위)
        }
    )

    source_collection = db['COL_STOCKPRICE_HISTORY']
    target_collection = db['COL_STOCKPRICE_HISTORY_TIME']

    migrate_to_timeseries(source_collection, target_collection)