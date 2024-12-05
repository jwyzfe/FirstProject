from typing import List
from pymongo import MongoClient
from pymongo.database import Database
from datetime import datetime, timedelta
import pytz

class QueueManager:
    """작업 큐 관리 클래스"""
    
    @staticmethod
    def get_work_collections(db: Database) -> List[str]:
        """_work로 끝나는 컬렉션 조회"""
        return [col for col in db.list_collection_names() if col.endswith('_WORK')]

    @staticmethod
    def delete_completed_jobs(db: Database, collection_name: str):
        """완료된 작업 삭제"""
        collection = db[collection_name]
        
        # fin 상태의 작업 찾기
        fin_jobs = collection.find({'ISWORK': 'fin'})
        
        for fin_job in fin_jobs:
            try:
                # ref_id로 ready 작업과 함께 삭제
                collection.delete_many({
                    '_id': {'$in': [fin_job['_id'], fin_job['REF_ID']]}
                })
                print(f"Deleted completed job pair in {collection_name}: ready_id={fin_job['REF_ID']}, fin_id={fin_job['_id']}")
            except Exception as e:
                print(f"Error deleting job pair in {collection_name}: {e}")

    @staticmethod
    def delete_old_ready_jobs(db: Database, collection_name: str, days: int = 7):
        """오래된 ready 상태 작업 삭제"""
        collection = db[collection_name]
        cutoff_date = datetime.now(pytz.UTC) - timedelta(days=days)
        
        try:
            # 오래된 ready 작업 찾아서 삭제
            result = collection.delete_many({
                'ISWORK': 'ready',
                'CREATED_AT': {'$lt': cutoff_date}
            })
            if result.deleted_count > 0:
                print(f"Deleted {result.deleted_count} old ready jobs in {collection_name}")
        except Exception as e:
            print(f"Error deleting old ready jobs in {collection_name}: {e}")

    @staticmethod
    def cleanup_work_collections(db: Database, days: int = 7):
        """작업 정리"""
        work_collections = QueueManager.get_work_collections(db)
        
        for collection_name in work_collections:
            print(f"\nProcessing collection: {collection_name}")
            # 완료된 작업 삭제
            QueueManager.delete_completed_jobs(db, collection_name)
            # 오래된 ready 작업 삭제
            QueueManager.delete_old_ready_jobs(db, collection_name, days)

if __name__ == "__main__":
    client = MongoClient('mongodb://192.168.0.91:27017/')
    db = client['DB_TEST']
    
    # 7일 이상 된 작업 정리
    QueueManager.cleanup_work_collections(db, days=7)