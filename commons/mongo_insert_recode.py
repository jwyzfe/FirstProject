import pandas as pd
from datetime import datetime 
import pytz

class connect_mongo:

    def insert_recode_in_mongo(client, dbname, collectionname, input_list):
        # 데이터베이스 선택
        db = client[dbname]  
        collection = db[collectionname]

        # 현재 시간 기록
        current_time = datetime.now(pytz.timezone('Asia/Seoul')) # 왜 3시간 느리지 mac이라서? 

        # 데이터 입력
        if isinstance(input_list, pd.DataFrame):
            records = input_list.to_dict(orient='records')
            for record in records:
                record['created_at'] = current_time  # 입력 시점 추가
            results = collection.insert_many(records)
        elif isinstance(input_list, list):
            for record in input_list:
                record['created_at'] = current_time  # 입력 시점 추가
            results = collection.insert_many(input_list)
        elif isinstance(input_list, dict):
            input_list['created_at'] = current_time  # 입력 시점 추가
            results = collection.insert_one(input_list)
        else:
            print("insert_recode_in_mongo: type error")
            return None
        

        return results
    
    def insert_recode_in_mongo_notime(client, dbname, collectionname, input_list):

        # 'mydatabase' 데이터베이스 선택 (없으면 자동 생성)
        db = client[dbname]
        # 'users' 컬렉션 선택 (없으면 자동 생성)
        collection = db[collectionname]

        # 데이터 입력
        if isinstance(input_list, pd.DataFrame):
            records = input_list.to_dict(orient='records')
            results = collection.insert_many(records)
        elif isinstance(input_list, list):
            results = collection.insert_many(input_list)
        elif isinstance(input_list, dict):
            results = collection.insert_one(input_list)
        else:
            print("insert_recode_in_mongo: type error")
            return None

        return results
