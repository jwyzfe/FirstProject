from pymongo import MongoClient
from datetime import datetime, timedelta


def migrate_to_timeseries_embedded(source_collection, target_collection, batch_size=1000):
    # _id로 정렬하여 순차적으로 처리
    last_id = None
    
    while True:
        # 쿼리 조건 설정
        query = {} if last_id is None else {'_id': {'$gt': last_id}}
        
        # 배치 크기만큼 데이터 가져오기
        batch = list(source_collection.find(query)
                    .limit(batch_size)
                    .sort('_id', 1))
        
        if not batch:
            break
            
        # 데이터 변환 및 삽입
        documents = []
        for doc in batch:
            # _id 필드 제외
            doc.pop('_id', None)
            
            # STOCK SPLITS 필드 이름 변경
            if 'STOCK SPLITS' in doc:
                doc['STOCKSPLITS'] = doc.pop('STOCK SPLITS')
            
            # 임베디드 문서로 구성할 필드들
            price_data = {
                "OPEN": doc["OPEN"],
                "HIGH": doc["HIGH"],
                "LOW": doc["LOW"],
                "CLOSE": doc["CLOSE"],
                "VOLUME": doc["VOLUME"],
                "STOCKSPLITS": doc.get("STOCKSPLITS", 0),  # 기본값 0
                "DIVIDENDS": doc.get("DIVIDENDS", 0)       # 기본값 0
            }
            
            # 새로운 문서 구조
            new_doc = {
                "SYMBOL": doc["SYMBOL"],
                "DATE": doc["DATE"],          # 타임시리즈 필드
                "price_data": price_data,     # 임베디드 문서
                "CREATED_AT": doc.get("CREATED_AT", datetime.utcnow())
            }
            
            documents.append(new_doc)
        
        # 배치 삽입
        if documents:
            target_collection.insert_many(documents)
            print(f"Migrated {len(documents)} documents")
        
        # 마지막 _id 저장
        last_id = batch[-1]['_id']


if __name__ == "__main__":
    # MongoDB 클라이언트 연결
    client = MongoClient('mongodb://192.168.0.50:27017/')
    db = client['DB_SGMN']

    # 타임시리즈 컬렉션 생성
    try:
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
    target_collection.create_index([("SYMBOL", 1), ("DATE", 1)], unique=True)

    # 데이터 마이그레이션 실행
    migrate_to_timeseries_embedded(source_collection, target_collection)