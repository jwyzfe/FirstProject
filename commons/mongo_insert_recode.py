import pandas as pd
from datetime import datetime 
import pytz
from typing import Union, Dict, List, Optional
from pymongo.results import InsertOneResult, InsertManyResult
from pymongo import MongoClient

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
    
    @staticmethod
    def insert_recode_in_mongo(
        client: MongoClient,
        dbname: str,
        collectionname: str,
        input_list: Union[pd.DataFrame, List[Dict], Dict]
    ) -> Optional[Union[InsertOneResult, InsertManyResult]]:
        """MongoDB에 데이터를 삽입하고 CREATED_AT 필드를 추가

        Args:
            client (MongoClient): MongoDB 클라이언트
            dbname (str): 데이터베이스 이름
            collectionname (str): 컬렉션 이름
            input_list (Union[pd.DataFrame, List[Dict], Dict]): 삽입할 데이터
                - pd.DataFrame: DataFrame을 레코드로 변환하여 삽입
                - List[Dict]: 딕셔너리 리스트를 삽입
                - Dict: 단일 딕셔너리를 삽입

        Returns:
            Optional[Union[InsertOneResult, InsertManyResult]]: 
                - InsertOneResult: 단일 문서 삽입 결과
                - InsertManyResult: 다수 문서 삽입 결과
                - None: 입력 데이터 타입 오류 시

        Note:
            모든 데이터에 'CREATED_AT' 필드가 자동으로 추가됨 (Asia/Seoul 시간대)
            모든 field 명을 대문자로 변경하여 삽입
        """
        db = client[dbname]  
        collection = db[collectionname]
        current_time = datetime.now(pytz.timezone('Asia/Seoul'))

        if isinstance(input_list, pd.DataFrame):
            input_list.columns = input_list.columns.str.upper()
            records = input_list.to_dict(orient='records')
            for record in records:
                record['CREATED_AT'] = current_time
            results = collection.insert_many(records)
        
        elif isinstance(input_list, list):
            uppercase_list = [connect_mongo.convert_keys_to_uppercase(record) for record in input_list]
            for record in uppercase_list:
                record['CREATED_AT'] = current_time
            results = collection.insert_many(uppercase_list)
        
        elif isinstance(input_list, dict):
            uppercase_dict = connect_mongo.convert_keys_to_uppercase(input_list)
            uppercase_dict['CREATED_AT'] = current_time
            results = collection.insert_one(uppercase_dict)
        
        else:
            print("insert_recode_in_mongo: type error")
            return None
        
        return results
    
    @staticmethod
    def insert_recode_in_mongo_notime(
        client: MongoClient,
        dbname: str,
        collectionname: str,
        input_list: Union[pd.DataFrame, List[Dict], Dict]
    ) -> Optional[Union[InsertOneResult, InsertManyResult]]:
        """MongoDB에 데이터를 삽입 (CREATED_AT 필드 없음)

        Args:
            client (MongoClient): MongoDB 클라이언트
            dbname (str): 데이터베이스 이름
            collectionname (str): 컬렉션 이름
            input_list (Union[pd.DataFrame, List[Dict], Dict]): 삽입할 데이터
                - pd.DataFrame: DataFrame을 레코드로 변환하여 삽입
                - List[Dict]: 딕셔너리 리스트를 삽입
                - Dict: 단일 딕셔너리를 삽입

        Returns:
            Optional[Union[InsertOneResult, InsertManyResult]]: 
                - InsertOneResult: 단일 문서 삽입 결과
                - InsertManyResult: 다수 문서 삽입 결과
                - None: 입력 데이터 타입 오류 시
        """
        db = client[dbname]
        collection = db[collectionname]

        if isinstance(input_list, pd.DataFrame):
            input_list.columns = input_list.columns.str.upper()
            records = input_list.to_dict(orient='records')
            results = collection.insert_many(records)
        elif isinstance(input_list, list):
            uppercase_list = [connect_mongo.convert_keys_to_uppercase(record) for record in input_list]
            results = collection.insert_many(uppercase_list)
        elif isinstance(input_list, dict):
            uppercase_dict = connect_mongo.convert_keys_to_uppercase(input_list)
            results = collection.insert_one(uppercase_dict)
        else:
            print("insert_recode_in_mongo: type error")
            return None

        return results