from pymongo import MongoClient
from datetime import datetime, timedelta


def migrate_to_timeseries(source_collection, target_collection, batch_size=1000):
    
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
            # _id와 CREATED_AT 필드 제외
            doc.pop('_id', None)
            # doc.pop('CREATED_AT', None)  # 필요없는 경우
                        
            # 필드 이름 변경
            if 'STOCK SPLITS' in doc:
                doc['STOCKSPLITS'] = doc.pop('STOCK SPLITS')  # 필드 이름 변경
            
            documents.append(doc)
        
        # 배치 삽입
        if documents:
            target_collection.insert_many(documents)
            print(f"Migrated {len(documents)} documents")
        
        # 마지막 _id 저장
        last_id = batch[-1]['_id']


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
            "granularity": "minutes"   # 시간 단위 (minutes가 가장 작은 단위)
        }
    )

    source_collection = db['COL_STOCKPRICE_HISTORY']
    target_collection = db['COL_STOCKPRICE_HISTORY_TIME']

    migrate_to_timeseries(source_collection, target_collection)