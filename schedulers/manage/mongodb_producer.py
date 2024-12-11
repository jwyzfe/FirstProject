from datetime import datetime
import pytz
from typing import List, Dict, Any, Callable
from pymongo import MongoClient
from pymongo.database import Database
import inspect

# main에서 다른 폴더 경로 참조하려면 필요
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from commons.mongo_insert_recode import connect_mongo

class JobProducer:
    """작업을 생성하고 등록하는 Producer 클래스"""

    @staticmethod
    def register_all_daily_jobs(source_db: Database, target_db: Database, client: MongoClient, jobs_config: Dict):
        """모든 일일 작업 등록

        Args:
            source_db (Database): 소스 데이터 데이터베이스
            target_db (Database): 작업 큐 데이터베이스
            client (MongoClient): MongoDB 클라이언트
            jobs_config (Dict): 작업 설정 정보

        Note:
            작업 유형별로 다음과 같이 처리:
            1. 소스 컬렉션 기반 작업 (yfinance, toss, stocktwits)
            2. URL 기반 작업 (hankyung)
            3. 반복 작업 (yahoo)
        """
        for job_type, config in jobs_config.items():
            print(f"\n{job_type} 작업 등록 중...")
            
            if config['collections']['source']:  # 소스 컬렉션이 있는 경우
                # 소스 데이터 조회
                source_data = list(source_db[config['collections']['source']].find())
                
                # 필터 적용 (있는 경우)
                if 'filter' in config['producer']:
                    source_data = list(filter(config['producer']['filter'], source_data))
                
                # 배치 크기 설정
                # batch_size = config['producer'].get('batch_size', len(source_data))
                # source_data = source_data[:batch_size]
                
                # 작업 데이터 생성 및 등록
                for item in source_data:
                    job_data = {}
                    
                    # symbol 필드 처리
                    if 'symbol_field' in config['producer']:
                        symbol_key = config['producer']['symbol_field']
                        job_data['SYMBOL'] = symbol_key(item) if callable(symbol_key) else item[symbol_key]
                    
                    JobProducer._insert_job(
                        target_db, 
                        config['collections']['work'],
                        job_data, 
                        client
                    )
            
            # URL 기반 작업 (hankyung)
            elif 'url_base' in config['producer']:
                for category in config['producer']['categories']:
                    for page in range(1, config['producer']['batch_size'] + 1):
                        url = config['producer']['url_base'].format(
                            category=category, 
                            page=page
                        )
                        JobProducer._insert_job(
                            target_db,
                            config['collections']['work'],
                            {'URL': url},
                            client
                        )
            
            # 반복 작업 (yahoo)
            elif 'count' in config['producer']:
                for _ in range(config['producer']['count']):
                    JobProducer._insert_job(
                        target_db,
                        config['collections']['work'],
                        {},
                        client
                    )
            
            print(f"{job_type} 작업 등록 완료")

    @staticmethod
    def _insert_job(db: Database, collection_name: str, job_data: Dict, client: MongoClient):
        """작업을 작업 큐에 등록

        Args:
            db (Database): 작업 큐 데이터베이스
            collection_name (str): 작업 큐 컬렉션 이름
            job_data (Dict): 작업 데이터
            client (MongoClient): MongoDB 클라이언트
        """
        if not collection_name:  # 빈 문자열이면 건너뛰기
            return
            
        job_data['ISWORK'] = 'ready'
        connect_mongo.insert_recode_in_mongo(
            client=client,
            dbname=db.name,
            collectionname=collection_name,
            input_list=job_data
        )


if __name__ == "__main__":
    
    # 작업 유형별 설정
    JOBS_CONFIG = {
        'yfinance': {
            'collection': 'COL_STOCKPRICE_DAILY_WORK',
            'source': {
                'collection': 'COL_NAS25_KOSPI25_CORPLIST',
                'symbol_field': 'SYMBOL'
            },
            'batch_size': 20
        },
        'toss': {
            'collection': 'COL_SCRAPPING_TOSS_COMMENT_DAILY_WORK',
            'source': {
                'collection': 'COL_NAS25_KOSPI25_CORPLIST',
                'symbol_field': lambda corp: corp['SYMBOL_KS'] if corp['MARKET'] == 'kospi' else corp['SYMBOL']
            },
            'batch_size': 5
        },
        'stocktwits': {
            'collection': 'COL_SCRAPPING_STOCKTWITS_COMMENT_DAILY_WORK',
            'source': {
                'collection': 'COL_NAS25_KOSPI25_CORPLIST',
                'symbol_field': 'SYMBOL',
                'filter': lambda corp: corp['MARKET'] == 'nasdaq'
            },
            'batch_size': 1
        },
        'yahoofinance': {
            'collection': 'COL_YAHOOFINANCE_DAILY_WORK',
            'count': 10,
            'batch_size': 1
        },
        'hankyung': {
            'collection': 'COL_SCRAPPING_HANKYUNG_DAILY_WORK',
            'url_base': 'https://www.hankyung.com/{category}?page={page}',
            'categories': ['economy', 'financial-market', 'industry', 
                         'politics', 'society', 'international'],
            'batch_size': 500
        },
        'financial': {
            'collection': 'COL_FINANCIAL_DAILY_WORK',
            'source': {
                'collection': 'COL_FINANCIAL_CORPLIST',
                'code_field': 'CORP_REGIST_NUM'
            },
            'batch_size': 10
        }
    }
    
    client_source = MongoClient('mongodb://192.168.0.50:27017/')
    source_db = client_source['DB_SGMN']

    client_target = MongoClient('mongodb://192.168.0.91:27017/')
    target_db = client_target['DB_TEST']

    JobProducer.register_all_daily_jobs(
        source_db=source_db,
        target_db=target_db,
        client=client_target,
        jobs_config=JOBS_CONFIG
    )

