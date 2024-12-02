from pymongo import MongoClient
from datetime import datetime, timedelta


def migrate_to_timeseries_embedded(source_collection, target_collection, batch_size=1000):
    last_id = None
    total_migrated = 0
    
    while True:
        # 단순히 _id로만 쿼리
        query = {} if last_id is None else {'_id': {'$gt': last_id}}
        
        # _id로만 정렬
        batch = list(source_collection.find(query)
                    .limit(batch_size)
                    .sort('_id', 1))
        
        if not batch:
            break
            
        documents = []
        for doc in batch:
            try:
                # _id 저장 후 제거
                last_id = doc['_id']
                doc.pop('_id', None)
                
                # STOCK SPLITS 필드 이름 변경
                if 'STOCK SPLITS' in doc:
                    doc['STOCKSPLITS'] = doc.pop('STOCK SPLITS')
                
                price_data = {
                    "OPEN": doc["OPEN"],
                    "HIGH": doc["HIGH"],
                    "LOW": doc["LOW"],
                    "CLOSE": doc["CLOSE"],
                    "VOLUME": doc["VOLUME"],
                    "STOCKSPLITS": doc.get("STOCKSPLITS", 0),
                    "DIVIDENDS": doc.get("DIVIDENDS", 0)
                }
                
                new_doc = {
                    "SYMBOL": doc["SYMBOL"],
                    "DATE": doc["DATE"],
                    "price_data": price_data,
                    "CREATED_AT": doc.get("CREATED_AT", datetime.utcnow())
                }
                
                documents.append(new_doc)
                
            except Exception as e:
                print(f"Error processing document: {e}")
                continue
        
        if documents:
            total_migrated += len(documents)
            target_collection.insert_many(documents)
            print(f"Migrated batch: {len(documents)}, Total: {total_migrated}")


if __name__ == "__main__":
    # MongoDB 클라이언트 연결
    client = MongoClient('mongodb://192.168.0.50:27017/')
    db = client['DB_SGMN']

    # 타임시리즈 컬렉션 생성
    try:
        # 기존 컬렉션이 있다면 삭제
        if "COL_STOCKPRICE_TIMESERIES_EMBEDDED" in db.list_collection_names():
            print("Dropping existing collection and its buckets...")
            db.drop_collection("COL_STOCKPRICE_TIMESERIES_EMBEDDED")
            
            # 버킷이 모두 삭제되었는지 확인
            system_buckets = db.list_collection_names(filter={"name": {"$regex": "^system.buckets"}})
            if any("COL_STOCKPRICE_TIMESERIES_EMBEDDED" in bucket for bucket in system_buckets):
                print("Warning: Some buckets might still exist")
            else:
                print("All buckets successfully removed")

        db.create_collection(
            "COL_STOCKPRICE_TIMESERIES_EMBEDDED",
            timeseries = {
                "timeField": "DATE",        # 시간 필드
                "metaField": "SYMBOL",      # 메타데이터 필드
                "granularity": "minutes"    # 시간 단위
            }
        )
        print("Created new timeseries collection")
    except Exception as e:
        print(f"Collection already exists or error occurred: {e}")

    # 컬렉션 설정
    source_collection = db['COL_STOCKPRICE_HISTORY']
    target_collection = db['COL_STOCKPRICE_TIMESERIES_EMBEDDED']

    # 인덱스 생성
    target_collection.create_index([("SYMBOL", 1)])
    target_collection.create_index([("DATE", 1)])
    # target_collection.create_index([("SYMBOL", 1), ("DATE", 1)], unique=True)

    # 데이터 마이그레이션 실행
    migrate_to_timeseries_embedded(source_collection, target_collection)