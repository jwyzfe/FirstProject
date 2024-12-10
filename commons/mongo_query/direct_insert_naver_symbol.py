from pymongo import MongoClient
# commons 폴더의 공용 insert 모듈 import
import sys
import os
# 현재 파일의 두 단계 상위 디렉토리(FirstProject)를 path에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))  # /manage
parent_dir = os.path.dirname(current_dir)  # /schedulers
project_dir = os.path.dirname(parent_dir)  # /FirstProject
sys.path.append(project_dir)
from commons.config_reader import read_config

class SymbolUpdater:
    
    @staticmethod
    def create_symbol_mapping():
        # NASDAQ 심볼 매핑
        nasdaq_mapping = {
            'CRM': 'CRM',
            'BAC': 'BAC',
            'JNJ': 'JNJ',
            'PG': 'PG',
            'HD': 'HD',
            'MA': 'MA',
            'XOM': 'XOM',
            'UNH': 'UNH',
            'V': 'V',
            'LLY': 'LLY',
            'JPM': 'JPM',
            'WMT': 'WMT',
            'BRK-B': 'BRKb',
            'ORCL': 'ORCL.K',
            'ABBV': 'ABBV.K',
            'NVDA': 'NVDA.O',
            'AAPL': 'AAPL.O',
            'MSFT': 'MSFT.O',
            'AMZN': 'AMZN.O',
            'GOOG': 'GOOG.O',
            'META': 'META.O',
            'TSLA': 'TSLA.O',
            'NFLX': 'NFLX.O',
            'COST': 'COST.O',
            'AVGO': 'AVGO.O'
        }
        
        # KOSPI 심볼 매핑
        kospi_mapping = {
            'PKX': '005490',
            'SHG': '055550',
            'KB': '105560',
            'KEP': '015760',
            'HYMTF': '005380'
        }
        
        return nasdaq_mapping, kospi_mapping

    @staticmethod
    def update_symbols(client, db_name, collection_name):
        # 매핑 데이터 가져오기
        nasdaq_mapping, kospi_mapping = SymbolUpdater.create_symbol_mapping()
        
        # 컬렉션 가져오기
        collection = client[db_name][collection_name]
        
        # 모든 문서에 대해 업데이트 수행
        for doc in collection.find():
            symbol = doc.get('SYMBOL')
            symbol_ks = doc.get('SYMBOL_KS', '')  # SYMBOL_KS 필드 가져오기
            updates = {}
            
            # SYMBOL_N 업데이트
            if symbol in nasdaq_mapping:
                updates['SYMBOL_N'] = nasdaq_mapping[symbol]
            elif symbol in kospi_mapping:
                updates['SYMBOL_N'] = kospi_mapping[symbol]
            elif symbol_ks and len(symbol_ks) == 6 and symbol_ks.isdigit():
                # KOSPI 숫자 심볼을 그대로 사용
                updates['SYMBOL_N'] = symbol_ks
            
            # 업데이트할 내용이 있는 경우에만 업데이트 수행
            if updates:
                collection.update_one(
                    {'_id': doc['_id']},
                    {'$set': updates}
                )

def main():
    # MongoDB 연결 설정
    config = read_config()
    ip_add = config['MongoDB_remote_readonly']['ip_add']
    db_name = config['MongoDB_remote_readonly']['db_name']
    collection_name = 'COL_NAS25_KOSPI25_CORPLIST'
    
    # MongoDB 클라이언트 생성
    client = MongoClient(ip_add)
    
    try:
        # 심볼 업데이트 실행
        SymbolUpdater.update_symbols(client, db_name, collection_name)
        print("Symbol update completed successfully")
    except Exception as e:
        print(f"Error occurred: {str(e)}")
    finally:
        client.close()

if __name__ == "__main__":
    main()