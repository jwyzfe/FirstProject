import pandas as pd
from datetime import datetime 

class connect_mongo:

    # def insert_recode_in_mongo(client, dbname, collectionname, input_list):
    #     # 데이터베이스 선택
    #     db = client[dbname]  
    #     collection = db[collectionname]

    #     # 현재 시간 기록 => 다른 스케쥴러가 사용하는 시간과 같은지 확인 필요 
    #     current_time = datetime.now(pytz.timezone('Asia/Seoul'))

    #     try:
    #         # 딕셔너리 리스트 처리
    #         if isinstance(input_list, list): 
    #             for record in input_list:
    #                 record['created_at'] = current_time  
    #             results = collection.insert_many(input_list)

    #         # 단일 DataFrame 처리
    #         elif isinstance(input_list, pd.DataFrame):
    #             df_with_date = input_list.reset_index() # 일단 둠 더 생각 해보기 
    #             records = df_with_date.to_dict(orient='records')
    #             for record in records:
    #                 record['created_at'] = current_time
    #             results = collection.insert_many(records)
    #             return results

    #         # 단일 딕셔너리 처리
    #         elif isinstance(input_list, dict):
    #             input_list['created_at'] = current_time
    #             results = collection.insert_one(input_list)
    #             return results

    #         else:
    #             print("Error: Unsupported input type. Expected DataFrame list, DataFrame, or dict")
    #             return None

    #     except Exception as e:
    #         print(f"Error during MongoDB insertion: {str(e)}")
    #         return None
        
    # 모든 레코드 읽어오기
    @classmethod
    def get_records_cursor(cls, client, dbname, collectionname, find_key=None):
        
        # 데이터베이스 선택
        db = client[dbname]  
        collection = db[collectionname]
        try:
            records_cursor = collection.find()   # 모든 레코드 가져오기
        except Exception as e:
            print(f"Error reading records: {e}")
            client.close()  # 클라이언트 연결 종료

        return records_cursor
        
    # 모든 레코드 읽어오기
    @classmethod
    def get_records_dataframe(cls, client, dbname, collectionname, find_key=None):
        
        # 데이터베이스 선택
        db = client[dbname]  
        collection = db[collectionname]
        try:
            if find_key is None:
                # find_key가 없으면 모든 레코드 가져오기
                records_df = pd.DataFrame(list(collection.find()))
            else:
                # find_key가 있으면 해당 조건으로 필터링하여 가져오기
                records_df = pd.DataFrame(list(collection.find(find_key)))
            
        except Exception as e:
            print(f"Error reading records: {e}")
            client.close()  # 클라이언트 연결 종료

        return records_df

    @classmethod
    def get_unfinished_ready_records(cls, client, db_name, col_name):
        # 1. 먼저 완료된(fin) 레코드들의 ref_id 목록을 가져옵니다
        finished_records = cls.get_records_dataframe(
            client, 
            db_name, 
            col_name, 
            {"iswork": "fin"}
        )
        finished_ref_ids = [] if finished_records.empty else finished_records['ref_id'].tolist()

        # 2. ready 상태이면서 아직 완료되지 않은(ref_id가 finished_ref_ids에 없는) 레코드를 찾습니다
        ready_records = cls.get_records_dataframe(
            client, 
            db_name, 
            col_name, 
            {"iswork": "ready"}
        )
        
        if not ready_records.empty:
            # finished_ref_ids에 없는 _id를 가진 레코드만 필터링
            unfinished_records = ready_records[~ready_records['_id'].isin(finished_ref_ids)]
            return unfinished_records
        
        return pd.DataFrame()  
    