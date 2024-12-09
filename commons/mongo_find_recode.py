import pandas as pd
from datetime import datetime 
from typing import Optional, Dict, Any
from pymongo import MongoClient
from pymongo.cursor import Cursor

class connect_mongo:

    @classmethod
    def get_records_cursor(
        cls,
        client: MongoClient,
        dbname: str,
        collectionname: str,
        find_key: Optional[Dict] = None
    ) -> Cursor:
        """MongoDB 컬렉션에서 모든 레코드를 커서 형태로 반환

        Args:
            client (MongoClient): MongoDB 클라이언트
            dbname (str): 데이터베이스 이름
            collectionname (str): 컬렉션 이름
            find_key (Optional[Dict], optional): 검색 조건. Defaults to None.

        Returns:
            Cursor: MongoDB 커서 객체

        Raises:
            Exception: MongoDB 쿼리 실행 중 오류 발생
        """
        db = client[dbname]  
        collection = db[collectionname]
        try:
            records_cursor = collection.find()
        except Exception as e:
            print(f"Error reading records: {e}")
            raise

        return records_cursor
        
    @classmethod
    def get_records_dataframe(
        cls,
        client: MongoClient,
        dbname: str,
        collectionname: str,
        find_key: Optional[Dict] = None
    ) -> pd.DataFrame:
        """MongoDB 컬렉션에서 레코드를 DataFrame 형태로 반환

        Args:
            client (MongoClient): MongoDB 클라이언트
            dbname (str): 데이터베이스 이름
            collectionname (str): 컬렉션 이름
            find_key (Optional[Dict], optional): 검색 조건. Defaults to None.

        Returns:
            pd.DataFrame: 검색된 레코드를 포함하는 DataFrame
                - 빈 결과의 경우 빈 DataFrame 반환

        Raises:
            Exception: MongoDB 쿼리 실행 중 오류 발생
        """
        db = client[dbname]  
        collection = db[collectionname]
        try:
            if find_key is None:
                records_df = pd.DataFrame(list(collection.find()))
            else:
                records_df = pd.DataFrame(list(collection.find(find_key)))
            
        except Exception as e:
            print(f"Error reading records: {e}")
            raise

        return records_df

    @classmethod
    def get_unfinished_ready_records(
        cls,
        client: MongoClient,
        db_name: str,
        col_name: str
    ) -> pd.DataFrame:
        """완료되지 않은 ready 상태의 레코드를 반환

        Args:
            client (MongoClient): MongoDB 클라이언트
            db_name (str): 데이터베이스 이름
            col_name (str): 컬렉션 이름

        Returns:
            pd.DataFrame: 미완료 ready 상태 레코드를 포함하는 DataFrame
                - ISWORK가 'ready'이면서
                - 완료된(ISWORK='fin') 레코드의 REF_ID에 포함되지 않는 레코드들
                - 결과가 없는 경우 빈 DataFrame 반환

        Note:
            - 완료된 레코드는 ISWORK='fin' 상태를 가짐
            - REF_ID는 작업 참조 ID로 사용됨
        """
        # 1. 완료된(fin) 레코드들의 ref_id 목록 조회
        finished_records = cls.get_records_dataframe(
            client, 
            db_name, 
            col_name, 
            {"ISWORK": "fin"}
        )
        finished_ref_ids = [] if finished_records.empty else finished_records['REF_ID'].tolist()

        # 2. ready 상태이면서 미완료된 레코드 조회
        ready_records = cls.get_records_dataframe(
            client, 
            db_name, 
            col_name, 
            {"ISWORK": "ready"}
        )
        
        if not ready_records.empty:
            unfinished_records = ready_records[~ready_records['_id'].isin(finished_ref_ids)]
            return unfinished_records
        
        return pd.DataFrame()