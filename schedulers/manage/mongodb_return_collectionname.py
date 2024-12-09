
from pymongo import MongoClient

def get_collection_names(db_name):
    # MongoDB 클라이언트 생성
    client = MongoClient('mongodb://192.168.0.50:27017/')  # MongoDB 서버 주소
    db = client[db_name]  # 데이터베이스 선택

    # 컬렉션 이름 리스트 가져오기
    collection_names = db.list_collection_names()
    
    return collection_names

# 사용 예시
db_name = 'DB_SGMN'  # 데이터베이스 이름
collections = get_collection_names(db_name)
# print(collections)

# DAILY와 WORK로 끝나는 컬렉션 이름을 찾기
daily_collections = [name for name in collections if name.endswith('DAILY')]
work_collections = [name for name in collections if not name.endswith('WORK')]

# 결과 출력
# print("DAILY Collections:", daily_collections)
print("WORK Collections:", work_collections)


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
