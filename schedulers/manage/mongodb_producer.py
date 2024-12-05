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
    def get_source_data(source_db: Database, collection: str, filter_func: Callable = None) -> List[Dict]:
        """소스 데이터 조회"""
        data = list(source_db[collection].find())
        if filter_func:
            data = list(filter(filter_func, data))
        return data

    @staticmethod
    def create_job_data(config: Dict, source_item: Dict = None) -> Dict:
        """작업 데이터 생성"""
        job_data = {}
        
        if source_item:
            for param in config.get('params', []):
                if param == 'symbol':
                    symbol_key = config['symbol_key']
                    if callable(symbol_key):
                        job_data['symbol'] = symbol_key(source_item)
                    else:
                        job_data['symbol'] = source_item[symbol_key]
                elif param == 'registcode':
                    job_data['registcode'] = source_item[config['registcode_key']]
        
        return job_data

    @staticmethod
    def register_job(target_db: Database, collection_name: str, job_data: Dict[str, Any], client: MongoClient):
        """단일 작업 등록 메서드"""
        job_data['ISWORK'] = 'ready'
        connect_mongo.insert_recode_in_mongo(
            client=client,
            dbname=target_db.name,
            collectionname=collection_name,
            input_list=job_data
        )

    @staticmethod
    def register_jobs(source_db: Database, target_db: Database, 
                    job_type: str, collections: Dict[str, str],
                    job_configs: Dict[str, Dict], client: MongoClient):
        """작업 등록 통합 메서드"""
        config = job_configs[job_type]
        target_collection = collections[job_type]
        
        # 소스 데이터가 필요한 경우
        if 'source_collection' in config:
            source_data = JobProducer.get_source_data(
                source_db,
                collections[config['source_collection']],
                config.get('market_filter')
            )
            
            for item in source_data:
                job_data = JobProducer.create_job_data(config, item)
                JobProducer.register_job(target_db, target_collection, job_data, client)
        
        # URL 기반 작업 (한경)
        elif 'url_pattern' in config:
            for category in config['categories']:
                for page in range(1, config['batch_size'] + 1):
                    url = config['url_pattern'].format(category=category, page=page)
                    JobProducer.register_job(target_db, target_collection, {'url': url}, client)
        
        # 반복 작업 (야후 파이낸스)
        elif 'count' in config:
            for _ in range(config['count']):
                JobProducer.register_job(target_db, target_collection, {}, client)

    @staticmethod
    def register_all_daily_jobs(source_db: Database, target_db: Database, 
                              collections: Dict[str, str], job_configs: Dict[str, Dict],
                              client: MongoClient):
        """모든 일일 작업 등록"""
        for job_type in job_configs.keys():
            print(f"Registering {job_type} jobs...")
            JobProducer.register_jobs(
                source_db=source_db,
                target_db=target_db,
                job_type=job_type,
                collections=collections,
                job_configs=job_configs,
                client=client
            )



if __name__ == "__main__":
    
    # 작업 유형별 설정
    JOB_CONFIGS = {
        'yfinance': {
            'source_collection': 'corp_list',
            'params': ['symbol'],
            'batch_size': 20,
            'symbol_key': 'SYMBOL'
        },
        'toss': {
            'source_collection': 'corp_list',
            'params': ['symbol'],
            'batch_size': 5,
            'symbol_key': lambda corp: corp['SYMBOL_KS'] if corp['MARKET'] == 'kospi' else corp['SYMBOL'],
            'market_filter': None
        },
        'stocktwits': {
            'source_collection': 'corp_list',
            'params': ['symbol'],
            'batch_size': 1,
            'symbol_key': 'SYMBOL',
            'market_filter': lambda corp: corp['MARKET'] == 'nasdaq'
        },
        'yahoofinance': {
            'params': [],
            'batch_size': 1,
            'count': 10
        },
        'hankyung': {
            'params': ['url'],
            'batch_size': 500,
            'categories': ['economy', 'financial-market', 'industry', 
                         'politics', 'society', 'international'],
            'url_pattern': 'https://www.hankyung.com/{category}?page={page}'
        },
        'financial': {
            'source_collection': 'financial_corp',
            'params': ['registcode'],
            'batch_size': 10,
            'registcode_key': 'CORP_REGIST_NUM'
        }
    }

    collections = {
        'corp_list': 'COL_NAS25_KOSPI25_CORPLIST',
        'financial_corp': 'COL_FINANCIAL_CORPLIST',
        'yfinance': 'COL_STOCKPRICE_DAILY_WORK',
        'toss': 'COL_SCRAPPING_TOSS_COMMENT_DAILY_WORK',
        'stocktwits': 'COL_SCRAPPING_STOCKTWITS_COMMENT_DAILY_WORK',
        'yahoofinance': 'COL_YAHOOFINANCE_DAILY_WORK',
        'hankyung': 'COL_SCRAPPING_HANKYUNG_DAILY_WORK',
        'financial': 'COL_FINANCIAL_DAILY_WORK'
    }
    
    client_source = MongoClient('mongodb://192.168.0.50:27017/')
    source_db = client_source['DB_SGMN']

    client_target = MongoClient('mongodb://192.168.0.91:27017/')
    target_db = client_target['DB_TEST']

    JobProducer.register_all_daily_jobs(
        source_db=source_db,
        target_db=target_db,
        collections=collections,
        job_configs=JOB_CONFIGS,
        client=client_target
    )

