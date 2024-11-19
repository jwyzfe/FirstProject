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
'''
        {"func" : api_stockprice_yfinance.get_stockprice_yfinance, "args" : "symbol", "target" : f'COL_STOCKPRICE_HISTORY', "work" : f'COL_STOCKPRICE_WORK'},
        {"func" : calc_market_senti.get_market_senti_list, "args" : "symbol", "target" : f'COL_MARKETSENTI_HISTORY', "work" : f'COL_MARKETSENTI_WORK'},
        {"func" : bs4_scrapping.bs4_news_hankyung, "args" : "url", "target" : f'COL_SCRAPPING_HANKYUNG_HISTORY', "work" : f'COL_SCRAPPING_HANKYUNG_WORK'},
        {"func" : CompanyFinancials.get_financial_statements, "args" : "corp_regist_num", "target" : f'COL_FINANCIAL_HISTORY', "work" : f'COL_FINANCIAL_WORK'}

'''
# 사용 예시
source_host = '192.168.0.48'  # A 컴퓨터의 IP 주소
source_port = 27017             # A 컴퓨터의 MongoDB 포트
source_db = 'DB_SGMN'              # A 컴퓨터의 데이터베이스 이름
source_collection = 'COL_STOCKPRICE_HISTORY'  # A 컴퓨터의 컬렉션 이름

dest_host = '192.168.0.50'     # B 컴퓨터의 IP 주소
dest_port = 27017               # B 컴퓨터의 MongoDB 포트
dest_db = 'DB_SGMN'                # B 컴퓨터의 데이터베이스 이름
dest_collection = 'COL_STOCKPRICE_HISTORY'  # B 컴퓨터의 컬렉션 이름

# 컬렉션 전송
transfer_collection(source_host, source_port, source_db, source_collection,
                    dest_host, dest_port, dest_db, dest_collection)