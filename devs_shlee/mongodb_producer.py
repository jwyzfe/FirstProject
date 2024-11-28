from datetime import datetime
import pytz
from typing import List, Dict, Any
from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection
from pymongo.database import Database

class JobProducer:
    """작업을 생성하고 등록하는 Producer 클래스"""
    
    @staticmethod
    def get_corp_symbols(db: Database, corp_list_collection: str = 'COL_NAS25_KOSPI25_CORPLIST') -> List[Dict]:
        """기업 목록 조회"""
        return list(db[corp_list_collection].find())

    @staticmethod
    def register_yfinance_jobs(db: Database, batch_size: int = 20):
        """yfinance 작업 등록 - symbol만 사용"""
        symbols = JobProducer.get_corp_symbols(db)
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            for corp in batch:
                JobProducer.register_job(db, 'COL_STOCKPRICE_DAILY_WORK', {
                    'symbol': corp['SYMBOL']
                })

    @staticmethod
    def register_toss_comment_jobs(db: Database, batch_size: int = 5):
        """toss comment 작업 등록 - MARKET에 따라 symbol 선택"""
        symbols = JobProducer.get_corp_symbols(db)
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            for corp in batch:
                symbol = corp['SYMBOL_KS'] if corp['MARKET'] == 'kospi' else corp['SYMBOL']
                JobProducer.register_job(db, 'COL_SCRAPPING_TOSS_COMMENT_DAILY_WORK', {
                    'symbol': symbol
                })

    @staticmethod
    def register_stocktwits_jobs(db: Database, batch_size: int = 1):
        """stocktwits 작업 등록 - NASDAQ 심볼만"""
        symbols = JobProducer.get_corp_symbols(db)
        nasdaq_symbols = [corp for corp in symbols if corp['MARKET'] == 'nasdaq']
        for i in range(0, len(nasdaq_symbols), batch_size):
            batch = nasdaq_symbols[i:i + batch_size]
            for corp in batch:
                JobProducer.register_job(db, 'COL_SCRAPPING_STOCKTWITS_COMMENT_DAILY_WORK', {
                    'symbol': corp['SYMBOL']
                })

    @staticmethod
    def register_yahoofinance_jobs(db: Database, count: int = 10):
        """yahoofinance 작업 등록 - 파라미터 없이 10번"""
        for _ in range(count):
            JobProducer.register_job(db, 'COL_YAHOOFINANCE_DAILY_WORK', {})

    @staticmethod
    def register_hankyung_jobs(db: Database, batch_size: int = 500):
        """한경 작업 등록 - URL 생성"""
        categories = ['economy', 'financial-market', 'industry', 
                     'politics', 'society', 'international']
        
        for category in categories:
            for page in range(1, batch_size + 1):
                url = f'https://www.hankyung.com/{category}?page={page}'
                JobProducer.register_job(db, 'COL_SCRAPPING_HANKYUNG_DAILY_WORK', {
                    'url': url
                })

    @staticmethod
    def register_financestate_jobs(db: Database, financial_corp_collection: str = 'COL_FINANCIAL_CORPLIST', batch_size: int = 10):
        """재무제표 작업 등록"""
        corps = list(db[financial_corp_collection].find())
        for i in range(0, len(corps), batch_size):
            batch = corps[i:i + batch_size]
            for corp in batch:
                JobProducer.register_job(db, 'COL_FINANCIAL_DAILY_WORK', {
                    'registcode': corp['CORP_REGIST_NUM']
                })

    @staticmethod
    def register_job(db: Database, collection_name: str, job_data: Dict[str, Any]) -> bool:
        """작업 등록 기본 메서드"""
        try:
            collection = db[collection_name]
            
            # 중복 검사
            query = {
                '$and': [
                    {'state': 'ready'},
                    {'$or': [
                        {'symbol': job_data.get('symbol')} if 'symbol' in job_data else {},
                        {'url': job_data.get('url')} if 'url' in job_data else {},
                        {'registcode': job_data.get('registcode')} if 'registcode' in job_data else {}
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
                'created_at': current_time
            }

            collection.insert_one(job_document)
            return True

        except Exception as e:
            print(f"Error registering job: {e}")
            return False

    @staticmethod
    def register_all_daily_jobs(db: Database):
        """모든 일일 작업 등록"""
        print("Registering YFinance jobs...")
        JobProducer.register_yfinance_jobs(db, batch_size=20)
        
        print("Registering Toss Comment jobs...")
        JobProducer.register_toss_comment_jobs(db, batch_size=5)
        
        print("Registering StockTwits jobs...")
        JobProducer.register_stocktwits_jobs(db, batch_size=1)
        
        print("Registering Hankyung jobs...")
        JobProducer.register_hankyung_jobs(db)
        
        print("Registering Finance State jobs...")
        JobProducer.register_financestate_jobs(db, batch_size=10)

# 사용 예시
if __name__ == "__main__":
    # 데이터베이스 연결
    client = MongoClient('mongodb://localhost:27017/')
    db = client['DB_SGMN']
    
    # 모든 일일 작업 등록
    JobProducer.register_all_daily_jobs(db)