from datetime import datetime, timedelta
import pytz
from typing import List, Dict, Any
from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection
from pymongo.database import Database

class JobProducer:
    """작업을 생성하고 등록하는 Producer 클래스"""
    
    def __init__(self, db: Database):
        self.db = db

    def get_work_collections(self) -> List[str]:
        """WORK로 끝나는 컬렉션 목록 조회"""
        collections = self.db.list_collection_names()
        return [name for name in collections if name.endswith('WORK')]

    '''
    작업을 등록하려면? 
    daily 먹이 여야함. 
    history는 굳이 생각 안해도 됨. 그냥 필요한 만큼 넣으면 되서.

    yfinance symbol, 6num.ks, COL_NAS25_KOSPI25_CORPLIST, COL_STOCKPRICE_DAILY_WORK
    tosscomment symbol, 6num, COL_NAS25_KOSPI25_CORPLIST, COL_SCRAPPING_TOSS_COMMENT_DAILY_WORK
    stocktwits symbol, COL_NAS25_KOSPI25_CORPLIST, COL_SCRAPPING_STOCKTWITS_COMMENT_DAILY_WORK
    marketsenti symbol, COL_NAS25_KOSPI25_CORPLIST, COL_MARKETSENTI_DAILY_WORK
    yahoofinance none 
    hankyung url making, url, page_list=['economy', 'financial-market', 'industry', 'politics', 'society', 'international'], f'https://www.hankyung.com/{classification}?page={page_num}', 1~500, COL_SCRAPPING_HANKYUNG_DAILY_WORK
    financestate registcode, COL_FINANCIAL_CORPLIST, COL_FINANCIAL_DAILY_WORK

    '''
    def register_job(self, collection_name: str, job_data: Dict[str, Any]) -> bool:
        """새로운 작업 등록"""
        try:
            collection = self.db[collection_name]
            
            # 중복 검사
            query = {
                '$and': [
                    {'state': 'ready'},
                    {'$or': [
                        {'symbol': job_data.get('symbol')} if 'symbol' in job_data else {},
                        {'url': job_data.get('url')} if 'url' in job_data else {}
                    ]}
                ]
            }
            
            if collection.find_one(query):
                print(f"Duplicate job found in {collection_name}: {job_data}")
                return False

            # 작업 등록
            current_time = datetime.now(pytz.UTC)
            job_document = {
                **job_data,
                'state': 'ready',
                'created_at': current_time,
                #'updated_at': current_time,
                #'attempts': 0
            }

            collection.insert_one(job_document)
            return True

        except Exception as e:
            print(f"Error registering job: {e}")
            return False

class QueueManager:
    """작업 큐를 관리하는 Manager 클래스"""
    
    def __init__(self, db: Database):
        self.db = db

    def get_work_collections(self) -> List[str]:
        """WORK로 끝나는 컬렉션 목록 조회"""
        collections = self.db.list_collection_names()
        return [name for name in collections if name.endswith('WORK')]

    def cleanup_stale_jobs(self, max_attempts: int = 3) -> Dict[str, int]:
        """
        오래된 작업 정리:
        - state가 'working'이고 fin되지 않은 작업 중
        - attempts가 max_attempts 이상인 작업을 삭제
        """
        cleanup_results = {}
        
        try:
            for collection_name in self.get_work_collections():
                collection = self.db[collection_name]
                
                # 삭제할 작업 찾기
                query = {
                    'state': 'working',
                    'attempts': {'$gte': max_attempts}
                }
                
                # 해당 작업들의 ready 레코드 삭제
                result = collection.delete_many(query)
                cleanup_results[collection_name] = result.deleted_count
                
                print(f"Cleaned up {result.deleted_count} stale jobs from {collection_name}")
                
        except Exception as e:
            print(f"Error cleaning up stale jobs: {e}")
        
        return cleanup_results

    def get_queue_stats(self) -> Dict[str, Dict]:
        """각 작업 큐의 상태 통계 조회"""
        stats = {}
        
        try:
            for collection_name in self.get_work_collections():
                collection = self.db[collection_name]
                
                stats[collection_name] = {
                    'ready': collection.count_documents({'state': 'ready'}),
                    'working': collection.count_documents({'state': 'working'}),
                    'fin': collection.count_documents({'state': 'fin'}),
                    'stale': collection.count_documents({
                        'state': 'working',
                        'attempts': {'$gte': 3}
                    })
                }
                
        except Exception as e:
            print(f"Error getting queue stats: {e}")
            
        return stats

# 사용 예시
if __name__ == "__main__":
    # MongoDB 연결
    client = MongoClient('mongodb://localhost:27017/')
    db = client['your_database']
    
    # Producer 생성 및 사용
    producer = JobProducer(db)
    
    # 작업 등록 예시
    job_data = {'symbol': 'AAPL', 'type': 'price_update'}
    for collection_name in producer.get_work_collections():
        producer.register_job(collection_name, job_data)
    
    # Queue Manager 생성 및 사용
    manager = QueueManager(db)
    
    # 오래된 작업 정리
    cleanup_results = manager.cleanup_stale_jobs()
    print("Cleanup results:", cleanup_results)
    
    # 큐 상태 확인
    queue_stats = manager.get_queue_stats()
    print("Queue stats:", queue_stats)