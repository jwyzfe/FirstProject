from pymongo import MongoClient

def rename_fields_to_uppercase(db_name, collection_name):
    client = MongoClient('mongodb://192.168.0.50:27017')
    db = client[db_name]
    collection = db[collection_name]

    # 컬렉션의 한 문서를 가져와서 필드명 확인
    sample_doc = collection.find_one()
    if not sample_doc:
        print("Collection is empty")
        return

    # _id를 제외한 모든 필드에 대해 rename 작업 수행
    rename_dict = {
        field: field.upper() 
        for field in sample_doc.keys() 
        if field != "_id" and field != field.upper()
    }

    if rename_dict:
        # 한 번의 업데이트로 모든 필드명 변경
        collection.update_many(
            {},
            {"$rename": rename_dict}
        )
        print(f"Renamed fields: {rename_dict}")
    else:
        print("No fields to rename")

if __name__ == "__main__":
    rename_fields_to_uppercase('DB_SGMN', 'COL_FINANCIAL_HISTORY')  # 데이터베이스와 컬렉션 이름 지정