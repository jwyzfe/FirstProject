import pandas as pd
from datetime import datetime 

class connect_mongo:
    @staticmethod
    def is_dataframe_list(input_list):
        """리스트의 모든 요소가 DataFrame인지 확인하는 함수"""
        return (
            isinstance(input_list, list) and 
            len(input_list) > 0 and 
            all(isinstance(item, pd.DataFrame) for item in input_list)
        )

    def insert_recode_in_mongo(client, dbname, collectionname, input_list):
        # 데이터베이스 선택
        db = client[dbname.lower()]  # 대소문자 구분 문제 방지
        collection = db[collectionname]

        # 현재 시간 기록
        current_time = datetime.now()

        try:
            # DataFrame 리스트 처리
            if connect_mongo.is_dataframe_list(input_list):
                all_records = []
                for df in input_list:
                    # 인덱스를 'Date' 컬럼으로 변환
                    df_with_date = df.reset_index()
                    # DataFrame을 레코드 리스트로 변환
                    records = df_with_date.to_dict(orient='records')
                    # 각 레코드에 생성 시간 추가
                    for record in records:
                        record['created_at'] = current_time
                    all_records.extend(records)
                
                if all_records:  # 레코드가 있는 경우에만 삽입
                    results = collection.insert_many(all_records)
                    return results
                else:
                    print("No records to insert")
                    return None

            # 단일 DataFrame 처리
            elif isinstance(input_list, pd.DataFrame):
                df_with_date = input_list.reset_index()
                records = df_with_date.to_dict(orient='records')
                for record in records:
                    record['created_at'] = current_time
                results = collection.insert_many(records)
                return results

            # 딕셔너리 처리
            elif isinstance(input_list, dict):
                input_list['created_at'] = current_time
                results = collection.insert_one(input_list)
                return results

            else:
                print("Error: Unsupported input type. Expected DataFrame list, DataFrame, or dict")
                return None

        except Exception as e:
            print(f"Error during MongoDB insertion: {str(e)}")
            return None