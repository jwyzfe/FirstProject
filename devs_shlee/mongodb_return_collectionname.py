
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
work_collections = [name for name in collections if name.endswith('WORK')]
'''
이게 되면 일괄 적으로 일감 생성하거나 
조회 하거나
정제 하는게 가능
'''
# 결과 출력
print("DAILY Collections:", daily_collections)
print("WORK Collections:", work_collections)

'''
순서
1. 리스트 받아오고 
2. daily 애들 각각 돌기
3. 하나당 중복 제거후 target에 insert 까지 insert 할 때 기존 db에 중복 인지도 보고 넣자 
4. 어찌 되었건 record 많을 때 빠르게 접근하는 거 방안 필요 
5. 일감 넣는 거 자동화는? 
6. work 에 일단 ready fin 쌍 지우는거 적용 해야 함.
7. 
8. daily랑 history는 다로 work 주는게 맞는 거 같음.
9. history는 그냥 파라미터 만 주면 될 거 같음.
10. daily는 파라미터를 계속 새로 주어야 해. => ready fin 으로 일시킬때 도는 시간 값같은거도 주어야함? 
    -> 뭔가 더 있어야 하긴할 듯 주기 별로 주도록 한다던가 시간을 지정해서 준다던가 왜냐면 그냥 일감 쌓아 놓는 식이면 안될거 같음 
11. 사실 history daily를 나눌 필요 없었는데, 파라미터 셋팅 잘 했었으면 

making work 
meterial db => work db
url, symbol, time, 
'''