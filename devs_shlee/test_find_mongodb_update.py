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
        '''
        임베디드 방식으로 바꿈 다른 db도 다 바꿔야해 특히 계속 데이터 들어오는 것들 daily도 다 맞추어야함. 
        
        '''
        for doc in batch:
            try:
                # _id 저장 후 제거
                last_id = doc['_id']
                doc.pop('_id', None)
                
                # STOCK SPLITS 필드 이름 변경
                if 'STOCK SPLITS' in doc:
                    doc['STOCKSPLITS'] = doc.pop('STOCK SPLITS')
                
                # 시간 데이터를 임베디드 문서로 구성
                time_data = {
                    "DATE": doc["DATE"],
                    "price_data": {
                        "OPEN": doc["OPEN"],
                        "HIGH": doc["HIGH"],
                        "LOW": doc["LOW"],
                        "CLOSE": doc["CLOSE"],
                        "VOLUME": doc["VOLUME"],
                        "STOCKSPLITS": doc.get("STOCKSPLITS", 0),
                        "DIVIDENDS": doc.get("DIVIDENDS", 0)
                    }
                }
                
                # SYMBOL을 최상위로
                new_doc = {
                    "SYMBOL": doc["SYMBOL"],
                    "time_data": time_data,
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
        if "COL_STOCKPRICE_EMBEDDED" in db.list_collection_names():
            print("Dropping existing collection and its buckets...")
            db.drop_collection("COL_STOCKPRICE_EMBEDDED")
            
        db.create_collection("COL_STOCKPRICE_EMBEDDED")
        print("Created new timeseries collection")
    except Exception as e:
        print(f"Collection already exists or error occurred: {e}")

    # 컬렉션 설정
    source_collection = db['COL_STOCKPRICE_HISTORY']
    target_collection = db['COL_STOCKPRICE_EMBEDDED']

    # 인덱스 생성
    # SYMBOL에 대한 인덱스 생성
    target_collection.create_index([("SYMBOL", 1)])
    # 복합 인덱스 생성
    target_collection.create_index([("SYMBOL", 1), ("time_data.DATE", 1)])

    # 데이터 마이그레이션 실행
    migrate_to_timeseries_embedded(source_collection, target_collection)