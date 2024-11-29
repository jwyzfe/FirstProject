from pymongo import MongoClient

def transfer_collection(source_host, source_port, source_db, source_collection,
                        dest_host, dest_port, dest_db, dest_collection):
    # A 컴퓨터의 MongoDB에 연결
    source_client = MongoClient(source_host, source_port)
    source_db = source_client[source_db]
    source_col = source_db[source_collection]

    # B 컴퓨터의 MongoDB에 연결
    dest_client = MongoClient(dest_host, dest_port)
    dest_db = dest_client[dest_db]
    dest_col = dest_db[dest_collection]

    # A 컴퓨터의 컬렉션에서 모든 문서 가져오기
    documents = source_col.find()

    # B 컴퓨터의 컬렉션에 문서 삽입
    for doc in documents:
        # _id 필드를 제거하여 중복 방지
        doc.pop('_id', None)  # _id 필드를 제거하여 중복 방지
        # doc.pop('iswork', None)  # iswork 필드를 제거
        dest_col.insert_one(doc)

    print(f"Transferred {source_col.count_documents({})} documents from {source_collection} to {dest_collection}.")


# 사용 예시
source_host = '192.168.0.50'  # A 컴퓨터의 IP 주소
source_port = 27017             # A 컴퓨터의 MongoDB 포트
source_db = 'DB_SGMN'              # A 컴퓨터의 데이터베이스 이름
source_collection = 'COL_SCRAPPING_STOCKTWITS_COMMENT_DAILY'  # A 컴퓨터의 컬렉션 이름  # 25000000 record 옮기는데 약 7시간 소요 => 배치로 돌리게 수정 해야 해. 아니면 걍 싹 다 새거로 받던가

dest_host = '192.168.0.91'     # B 컴퓨터의 IP 주소
dest_port = 27017               # B 컴퓨터의 MongoDB 포트
dest_db = 'DB_TEST'                # B 컴퓨터의 데이터베이스 이름
dest_collection = 'COL_SCRAPPING_STOCKTWITS_COMMENT_DAILY'  # B 컴퓨터의 컬렉션 이름

# 컬렉션 전송
transfer_collection(source_host, source_port, source_db, source_collection,
                    dest_host, dest_port, dest_db, dest_collection)



