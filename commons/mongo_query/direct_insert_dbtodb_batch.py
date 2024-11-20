from pymongo import MongoClient

def transfer_collection(source_host, source_port, source_db, source_collection,
                        dest_host, dest_port, dest_db, dest_collection, batch_size=10000):
    # A 컴퓨터의 MongoDB에 연결
    source_client = MongoClient(source_host, source_port)
    source_db = source_client[source_db]
    source_col = source_db[source_collection]

    # B 컴퓨터의 MongoDB에 연결
    dest_client = MongoClient(dest_host, dest_port)
    dest_db = dest_client[dest_db]
    dest_col = dest_db[dest_collection]

    # A 컴퓨터의 컬렉션에서 문서 수를 가져오기
    total_documents = source_col.count_documents({})
    print(f"Total documents to transfer: {total_documents}")

    # 배치로 문서 전송
    for skip in range(0, total_documents, batch_size):
        # 배치 크기만큼 문서 가져오기
        documents = source_col.find().skip(skip).limit(batch_size)

        # B 컴퓨터의 컬렉션에 문서 삽입
        for doc in documents:
            # _id 필드를 제거하여 중복 방지
            doc.pop('_id', None)  # _id 필드를 제거하여 중복 방지
            dest_col.insert_one(doc)

        print(f"Transferred documents from {skip} to {skip + batch_size}")

    print(f"Transferred {total_documents} documents from {source_collection} to {dest_collection}.")

# 사용 예시
source_host = '192.168.0.48'  # A 컴퓨터의 IP 주소
source_port = 27017             # A 컴퓨터의 MongoDB 포트
source_db = 'DB_SGMN'              # A 컴퓨터의 데이터베이스 이름
source_collection = 'COL_AMERICA_CORPLIST'  # A 컴퓨터의 컬렉션 이름

dest_host = '192.168.0.50'     # B 컴퓨터의 IP 주소
dest_port = 27017               # B 컴퓨터의 MongoDB 포트
dest_db = 'DB_SGMN'                # B 컴퓨터의 데이터베이스 이름
dest_collection = 'COL_AMERICA_CORPLIST'  # B 컴퓨터의 컬렉션 이름

# 컬렉션 전송
transfer_collection(source_host, source_port, source_db, source_collection,
                    dest_host, dest_port, dest_db, dest_collection)