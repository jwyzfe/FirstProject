from typing import List, Dict
from pymongo import MongoClient
from pymongo.database import Database

# commons 폴더의 공용 insert 모듈 import
import sys
import os
# 현재 파일의 두 단계 상위 디렉토리(FirstProject)를 path에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))  # /manage
parent_dir = os.path.dirname(current_dir)  # /schedulers
project_dir = os.path.dirname(parent_dir)  # /FirstProject
sys.path.append(project_dir)
from commons.time_utils import TimeUtils

class ResourceConsumer:
    """히스토리 컬렉션의 중복 데이터를 제거하는 클래스"""

    @staticmethod
    def remove_duplicates(db: Database, history_collection: str, dedup_collection: str, duplicate_fields: List[str]):
        """컬렉션 내 중복 데이터를 dedup 컬렉션으로 이동"""
        # 각 필드에 대한 참조 방식 결정
        field_refs = {}
        group_keys = {}
        for field in duplicate_fields:
            if "." in field:
                parent, child = field.split(".")
                field_refs[field] = {"$getField": {"field": child, "input": f"${parent}"}}
                group_keys[f"{parent}_{child}"] = field_refs[field]
            else:
                field_refs[field] = f"${field}"
                group_keys[field] = field_refs[field]
        
        pipeline = [
            {"$group": {
                "_id": group_keys,
                "docs": {"$push": "$$ROOT"},
                "count": {"$sum": 1}
            }},
            {"$match": {
                "count": {"$gt": 1}
            }}
        ]
        
        # 중복 데이터 찾기
        duplicates = list(db[history_collection].aggregate(pipeline))
        
        total_moved = 0
        # 각 중복 그룹 처리
        for dup in duplicates:
            # 첫 번째 문서를 제외한 나머지 문서들
            duplicate_docs = dup["docs"][1:]
            
            # dedup 컬렉션으로 이동할 문서들 준비
            docs_to_move = []
            ids_to_remove = []
            
            for doc in duplicate_docs:
                # 원본 _id 저장
                original_id = doc['_id']
                ids_to_remove.append(original_id)
                
                # dedup 컬렉션용 문서 준비
                doc['REF_ID'] = original_id  # 원본 문서의 ID 참조 추가
                doc['MOVED_AT'] = TimeUtils.get_current_time()  # 이동 시간 기록
                docs_to_move.append(doc)
            
            try:
                # dedup 컬렉션에 문서들 삽입
                if docs_to_move:
                    db[dedup_collection].insert_many(docs_to_move)
                    # history 컬렉션에서 문서들 삭제
                    db[history_collection].delete_many({"_id": {"$in": ids_to_remove}})
                    total_moved += len(docs_to_move)
            except Exception as e:
                print(f"Error processing duplicate group: {e}")
        
        if total_moved > 0:
            print(f"Moved {total_moved} duplicate documents from {history_collection} to {dedup_collection}")

    @staticmethod
    def process_all_history_collections(db: Database, collection_config: Dict):
        """모든 히스토리 컬렉션의 중복 제거 처리"""
        for job_type, config in collection_config.items():
            collections = config['collections']
            history_collection = collections.get('history')
            dedup_collection = collections.get('dedup')
            
            if not (history_collection and dedup_collection):
                continue
                
            print(f"\nProcessing {job_type} history collection: {history_collection}")
            
            # 중복 제거 실행
            duplicate_fields = config['integrator']['duplicate_fields']
            ResourceConsumer.remove_duplicates(
                db=db,
                history_collection=history_collection,
                dedup_collection=dedup_collection,
                duplicate_fields=duplicate_fields
            )


if __name__ == "__main__":
    # 데이터베이스 연결
    client = MongoClient('mongodb://192.168.0.91:27017/') # 연습 어떻게 함? 
    resource_db = client['DB_SGMN']
    
    # 아무리 그래도 너무 오래 걸림 자체 중복만 해결해서 넘기고 전체 중복은 아무래도 따로 하게 해야 할듯 
    PIPELINE_CONFIG = {
        'yfinance': {
            # 공통으로 사용할 컬렉션 정의
            'collections': {
                'source': 'COL_NAS25_KOSPI25_CORPLIST',      # 작업 생성용 소스 데이터
                'work': 'COL_STOCKPRICE_DAILY_WORK',  # 작업 큐
                'daily': 'COL_STOCKPRICE_DAILY',      # 일일 수집 데이터
                'history': 'COL_STOCKPRICE_EMBEDDED',   # 통합 저장소
                'dedup' : 'COL_STOCKPRICE_DEDUP'
            },
            # 아래 collection 주석들은 어떤 collection을 사용한다는 의미 실제 사용되는 코드 아님 
            # 작업 생성 설정 (Producer)
            'producer': {
                'symbol_field': 'SYMBOL',
                'batch_size': 20
                # source_collection: 'COL_NAS25_KOSPI25_CORPLIST'
                # work_collection: 'COL_STOCKPRICE_DAILY_WORK'
            },
            # 데이터 수집 설정 (Worker)
            'worker': {
                'function': api_stockprice_yfinance.get_stockprice_yfinance_daily,
                'param_field': 'SYMBOL',
                'schedule': 'minutes_10' # minutes_10
                # work_collection: 'COL_STOCKPRICE_DAILY_WORK'
                # target_collection: 'COL_STOCKPRICE_EMBEDDED_DAILY'
            },
            # 데이터 통합 설정 (Integrator)
            'integrator': {
                'duplicate_fields': ['SYMBOL', 'TIME_DATA.DATE']
                # source_collection: 'COL_STOCKPRICE_EMBEDDED_DAILY'
                # target_collection: 'COL_STOCKPRICE_EMBEDDED'
            }
        },
        'toss': {
            # 공통으로 사용할 컬렉션 정의
            'collections': {
                'source': 'COL_NAS25_KOSPI25_CORPLIST',      # 작업 생성용 소스 데이터
                'work': 'COL_SCRAPPING_TOSS_COMMENT_DAILY_WORK',  # 작업 큐
                'daily': 'COL_SCRAPPING_TOSS_COMMENT_DAILY',      # 일일 수집 데이터
                'history': 'COL_SCRAPPING_TOSS_COMMENT_HISTORY'   # 통합 저장소
            },
            # 아래 collection 주석들은 어떤 collection을 사용한다는 의미 실제 사용되는 코드 아님 
            # 작업 생성 설정 (Producer)
            'producer': {
                'symbol_field': lambda corp: corp['SYMBOL_KS'] if corp['MARKET'] == 'kospi' else corp['SYMBOL'],
                'batch_size': 5
                # source_collection: 'COL_NAS25_KOSPI25_CORPLIST'
                # work_collection: 'COL_SCRAPPING_TOSS_COMMENT_DAILY_WORK'
            },
            # 데이터 수집 설정 (Worker)
            'worker': {
                'function': scrap_toss_comment.run_toss_comments,
                'param_field': 'SYMBOL',
                'schedule': 'minutes_10' # minutes_10
                # work_collection: 'COL_SCRAPPING_TOSS_COMMENT_DAILY_WORK'
                # target_collection: 'COL_SCRAPPING_TOSS_COMMENT_DAILY'
            },
            # 데이터 통합 설정 (Integrator)
            'integrator': {
                'duplicate_fields': ['COMMENT', 'DATETIME']
                # source_collection: 'COL_SCRAPPING_TOSS_COMMENT_DAILY'
                # target_collection: 'COL_SCRAPPING_TOSS_COMMENT_HISTORY'
            }
        },
        'naver': {
            # 공통으로 사용할 컬렉션 정의
            'collections': {
                'source': 'COL_NAS25_KOSPI25_CORPLIST',      # 작업 생성용 소스 데이터
                'work': 'COL_SCRAPPING_NAVER_COMMENT_DAILY_WORK',  # 작업 큐
                'daily': 'COL_SCRAPPING_NAVER_COMMENT_DAILY',      # 일일 수집 데이터
                'history': 'COL_SCRAPPING_NAVER_COMMENT_HISTORY'   # 통합 저장소
            },
            # 아래 collection 주석들은 어떤 collection을 사용한다는 의미 실제 사용되는 코드 아님 
            # 작업 생성 설정 (Producer)
            'producer': {
                'symbol_field': 'SYMBOL_N',
                'batch_size': 5
                # source_collection: 'COL_NAS25_KOSPI25_CORPLIST'
                # work_collection: 'COL_SCRAPPING_TOSS_COMMENT_DAILY_WORK'
            },
            # 데이터 수집 설정 (Worker)
            'worker': {
                'function': all_print.get_symbol_list_to_reply,
                'param_field': 'SYMBOL',
                'schedule': 'minutes_10' # minutes_10
                # work_collection: 'COL_SCRAPPING_TOSS_COMMENT_DAILY_WORK'
                # target_collection: 'COL_SCRAPPING_TOSS_COMMENT_DAILY'
            },
            # 데이터 통합 설정 (Integrator)
            'integrator': {
                'duplicate_fields': ['CONTENT', 'DATE']
                # source_collection: 'COL_SCRAPPING_TOSS_COMMENT_DAILY'
                # target_collection: 'COL_SCRAPPING_TOSS_COMMENT_HISTORY'
            }
        },
        'stocktwits': {
            'collections': {
                'source': 'COL_NAS25_KOSPI25_CORPLIST',
                'work': 'COL_SCRAPPING_STOCKTWITS_COMMENT_DAILY_WORK',
                'daily': 'COL_SCRAPPING_STOCKTWITS_COMMENT_DAILY',
                'history': 'COL_SCRAPPING_STOCKTWITS_COMMENT_HISTORY'
            },
            'producer': {
                'symbol_field': 'SYMBOL',
                'filter': lambda corp: corp['MARKET'] == 'nasdaq',
                'batch_size': 3 # 1 
            },
            'worker': {
                'function': comment_scrap_stocktwits.run_stocktwits_scrap_list,
                'param_field': 'SYMBOL',
                'schedule': 'minutes_10' # minutes_10
            },
            'integrator': {
                'duplicate_fields': ['CONTENT', 'DATETIME']
            }
        },
        'yahoo': {
            'collections': {
                'source': '',  # 소스 컬렉션 없음 (직접 실행)
                'work': '',    # 작업 큐 없음
                'daily': 'COL_SCRAPPING_NEWS_YAHOO_DAILY',
                'history': 'COL_SCRAPPING_NEWS_YAHOO_HISTORY'
            },
            'producer': {
                'count': 10,
                'batch_size': 1
            },
            'worker': {
                'function': yahoo_finance_scrap.scrape_news_schedule_version,
                'param_field': 'SYMBOL',
                'schedule': 'hours_3' # hours_3
            },
            'integrator': {
                'duplicate_fields': ['NEWS_URL']
            }
        },
        'hankyung': {
            'collections': {
                'source': '',  # URL 기반 작업
                'work': 'COL_SCRAPPING_HANKYUNG_DAILY_WORK',
                'daily': 'COL_SCRAPPING_HANKYUNG_DAILY',
                'history': 'COL_SCRAPPING_HANKYUNG_HISTORY'
            },
            'producer': {
                'url_base': 'https://www.hankyung.com/{category}?page={page}',
                'categories': ['economy', 'financial-market', 'industry', 
                            'politics', 'society', 'international'],
                'batch_size': 10
            },
            'worker': {
                'function': bs4_scrapping.bs4_news_hankyung,
                'param_field': 'URL',
                'schedule': 'hours_3' # hours_3
            },
            'integrator': {
                'duplicate_fields': ['URL']  # 실제 중복 체크 필드 확인 필요
            }
        }
    }
    ResourceConsumer.process_all_history_collections(resource_db, PIPELINE_CONFIG)