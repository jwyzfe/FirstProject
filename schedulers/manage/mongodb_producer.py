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
        """모든 일일 작업 등록 (단순화된 버전)"""
        for job_type, config in jobs_config.items():
            print(f"{job_type} 작업 등록 중...")
            
            if 'source' in config:
                # 소스 데이터 기반 작업 (yfinance, toss, stocktwits, financial)
                source_data = list(source_db[config['source']['collection']].find())
                
                if 'filter' in config['source']:
                    source_data = list(filter(config['source']['filter'], source_data))
                
                for item in source_data:
                    job_data = {}
                    
                    if 'symbol_field' in config['source']:
                        symbol_key = config['source']['symbol_field']
                        job_data['symbol'] = symbol_key(item) if callable(symbol_key) else item[symbol_key]
                    
                    if 'code_field' in config['source']:
                        job_data['registcode'] = item[config['source']['code_field']]
                    
                    JobProducer._insert_job(target_db, config['collection'], job_data, client)
            
            elif 'url_base' in config:
                # URL 기반 작업 (hankyung)
                for category in config['categories']:
                    for page in range(1, config['batch_size'] + 1):
                        url = config['url_base'].format(category=category, page=page)
                        JobProducer._insert_job(target_db, config['collection'], {'url': url}, client)
            
            elif 'count' in config:
                # 반복 작업 (yahoofinance)
                for _ in range(config['count']):
                    JobProducer._insert_job(target_db, config['collection'], {}, client)

    @staticmethod
    def _insert_job(target_db: Database, collection: str, job_data: Dict, client: MongoClient):
        """작업 데이터 삽입"""
        job_data['ISWORK'] = 'ready'
        connect_mongo.insert_recode_in_mongo(
            client=client,
            dbname=target_db.name,
            collectionname=collection,
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

