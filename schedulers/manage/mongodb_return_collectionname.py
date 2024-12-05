
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

'''
1. 리스트 받아오고 
2. daily 애들 각각 돌기
4. 어찌 되었건 record 많을 때 빠르게 접근하는 거 방안 필요 
8. daily랑 history는 따로 work 주는게 맞는 거 같음.
9. history는 그냥 파라미터 만 주면 될 거 같음.
10. daily는 파라미터를 계속 새로 주어야 해. => ready fin 으로 일시킬때 도는 시간 값 같은거도 주어야함? 
    -> 뭔가 더 있어야 하긴할 듯 주기 별로 주도록 한다던가 시간을 지정해서 준다던가 왜냐면 그냥 일감 쌓아 놓는 식이면 안될거 같음 
11. 사실 history daily를 나눌 필요 없었는데, 파라미터 셋팅 잘 했었으면 

2. daily 에서 퍼와서 meterial에 쌓는 consumer     
    => 그냥 daily는 무조건 다 푸자 그러고 meterial에 넣을 때 중복만 잘 관리하자 
    => meterial이 무거우니 중복관리 따로 빼야 할지만 확인하고 가면 될 듯 

나중일 
cash 같은게 있음 좋긴하겠는데 cash를 따로 둘까 
갑자기 무서운게 일한 이력과 데이터가 같이 삭제 되면 그건 어떻하지 일한 이력은 남겨야 하나? 근대 용량이 없는데 그리고 굳이 있어야하나? 만일의 디버깅위해? 오버헤드 아냐? 

'''