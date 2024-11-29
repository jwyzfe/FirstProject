
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
7. 
8. daily랑 history는 따로 work 주는게 맞는 거 같음.
9. history는 그냥 파라미터 만 주면 될 거 같음.
10. daily는 파라미터를 계속 새로 주어야 해. => ready fin 으로 일시킬때 도는 시간 값같은거도 주어야함? 
    -> 뭔가 더 있어야 하긴할 듯 주기 별로 주도록 한다던가 시간을 지정해서 준다던가 왜냐면 그냥 일감 쌓아 놓는 식이면 안될거 같음 
11. 사실 history daily를 나눌 필요 없었는데, 파라미터 셋팅 잘 했었으면 

making work 
meterial db => work db
url, symbol, time, 

making은 일단 됨.

1. ready fin 지우고 working 관리하는 consumer => 큐관리 
2. daily 에서 퍼와서 meterial에 쌓는 consumer => 아 ready fin 그냥 지울게 아니라 meterial에 쌓을 때 지워야 하네 fin 된걸 넣어야 하잖아 
    => 근대 그럼 fin에 넣었던 id 기록해야해? => 그리고 계산같은거 미스나서 daily가 남으면 그건 또 어떻해? 걍 부으면 안되나 mutual만 신경쓰면 되려나 
    => 어차피 동시에는 못돌게 셋팅 하긴 햇어서 괜찮을 거 같긴한데 그래도 되는건가?


2. daily 에서 퍼와서 meterial에 쌓는 consumer     
    => 그냥 daily는 무조건 다 푸자 그러고 meterial에 넣을 때 중복만 잘 관리하자 
    => meterial이 무거우니 중복관리 따로 빼야 할지만 확인하고 가면 될 듯 


나중일 
=> daily를 보고 일해야 하는 경우가 있을까? => 매번 meterial 쓰는 것도 느리긴해서 cash 같은게 있음 좋긴하겠는데 => cash를 따로 둘까 
4. 갑자기 무서운게 일한 이력과 데이터가 같이 삭제 되면 그건 어떻하지 일한 이력은 남겨야 하나? 근대 용량이 없는데 그리고 굳이 있어야하나? 만일의 디버깅위해? 오버헤드 아냐? 
3. 일단 working 3번 돈 애들은 ready working *3 쌍을 따로 빼두자 다른 로거 db 만들어서 
차라리 error 나면 그 ready id를 다른 db에 넣으면 되네 그럼 컬렉션만 바꼇지 관리는 똑같은거 아님?  
그냥 실행여부와 error 여부는 그냥 아얘 다른애가 관리해야 하는 듯? 



'''