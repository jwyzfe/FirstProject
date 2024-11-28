import pandas as pd
from datetime import datetime 
import pytz

class connect_mongo:

    @staticmethod
    def convert_keys_to_uppercase(data):
        if isinstance(data, dict):
            return {k.upper(): connect_mongo.convert_keys_to_uppercase(v) if isinstance(v, (dict, list)) else v 
                    for k, v in data.items()}
        elif isinstance(data, list):
            return [connect_mongo.convert_keys_to_uppercase(item) if isinstance(item, (dict, list)) else item 
                    for item in data]
        return data
    
    def insert_recode_in_mongo(client, dbname, collectionname, input_list):
        db = client[dbname]  
        collection = db[collectionname]
        current_time = datetime.now(pytz.timezone('Asia/Seoul'))

        if isinstance(input_list, pd.DataFrame):
            # DataFrame의 컬럼명을 대문자로 변환
            input_list.columns = input_list.columns.str.upper()
            records = input_list.to_dict(orient='records')
            for record in records:
                record['CREATED_AT'] = current_time
            results = collection.insert_many(records)
        
        elif isinstance(input_list, list):
            # 리스트 내의 각 딕셔너리의 키를 대문자로 변환
            uppercase_list = [connect_mongo.convert_keys_to_uppercase(record) for record in input_list]
            for record in uppercase_list:
                record['CREATED_AT'] = current_time
            results = collection.insert_many(uppercase_list)
        
        elif isinstance(input_list, dict):
            # 딕셔너리의 키를 대문자로 변환
            uppercase_dict = connect_mongo.convert_keys_to_uppercase(input_list)
            uppercase_dict['CREATED_AT'] = current_time
            results = collection.insert_one(uppercase_dict)
        
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
            # DataFrame의 컬럼명을 대문자로 변환
            input_list.columns = input_list.columns.str.upper()
            records = input_list.to_dict(orient='records')
            results = collection.insert_many(records)
        elif isinstance(input_list, list):
            # 리스트 내의 각 딕셔너리의 키를 대문자로 변환
            uppercase_list = [connect_mongo.convert_keys_to_uppercase(record) for record in input_list]
            results = collection.insert_many(uppercase_list)
        elif isinstance(input_list, dict):
            # 딕셔너리의 키를 대문자로 변환
            uppercase_dict = connect_mongo.convert_keys_to_uppercase(input_list)
            results = collection.insert_one(uppercase_dict)
        else:
            print("insert_recode_in_mongo: type error")
            return None

        return results
