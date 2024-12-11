
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pymongo import MongoClient
from commons.mongo_insert_recode import connect_mongo as connect_mongo_insert             
from commons.config_reader import read_config
from commons.time_utils import TimeUtils
from commons.api_send_requester import ApiRequester

class KosisApiHandler:

    @staticmethod
    def get_params(index_config):
        config = read_config()
        apiKey = config['kosis']['api_key']
        
        # 현재 날짜를 YYYYMM 형식으로 가져오기
        endPrdDe = TimeUtils.get_current_time().strftime("%Y%m")
        
        # 파라미터 설정
        params = {
            "method": "getList",
            "apiKey": apiKey,
            "itmId": index_config["itmId"],
            "format": "json",
            "jsonVD": "Y",
            "prdSe": "M",
            "startPrdDe": index_config["startPrdDe"],
            "endPrdDe": endPrdDe,
            "orgId": index_config["orgId"],
            "tblId": index_config["tblId"]
        }
        
        # objL1 ~ objL8 설정
        for i in range(1, 9):
            key = f"objL{i}"
            params[key] = index_config.get(key, "")
        
        return params

    @staticmethod
    def fetch_data(index_name, index_config):
        base_url = 'https://kosis.kr/openapi/Param/statisticsParameterData.do'
        params = KosisApiHandler.get_params(index_config)
        result_data = ApiRequester.send_api(base_url, params)
        return result_data

    @staticmethod
    def fetch_all_data(index_settings):
        results = {}
        for index_name, index_config in index_settings.items():
            data = KosisApiHandler.fetch_data(index_name, index_config)
            results[index_name] = data
        return results

if __name__ == "__main__":
    # 외부에서 INDEX_SETTINGS를 전달받는 예시
    # 데이터 많지 않으니 그냥 째로 넘겨주고 실행하도록 할까?
    # 어차피 2주에 한번 정도 일거니까? 또 엄청 많이는 안할 거니까? 
    INDEX_SETTINGS = {
        "Composite_Economic_Index": {
            "itmId": "T1",
            "orgId": 101,
            "tblId": "DT_1C8015",
            "objL1": "ALL",
            "objL2": "",
            "startPrdDe": "197001",
            "collection_name": "Composite_Economic_Index"
        },
        "Economic_Sentiment_Index": {
            "itmId": "13103134473999",
            "orgId": 301,
            "tblId": "DT_513Y001",
            "objL1": "ALL",
            "startPrdDe": "200301",
            "collection_name": "Economic_Sentiment_Index"
        }
    }

    ip_add = f'mongodb://192.168.0.91:27017/'
    db_name = f'DB_SGMN' # db name 바꾸기
    # col_name = f'COL_NAVER_STOCK_REPLY' # collection name 바꾸기
    
    # MongoDB 서버에 연결
    client = MongoClient(ip_add)

    # 모든 지수에 대한 데이터 가져오기
    all_data = KosisApiHandler.fetch_all_data(INDEX_SETTINGS)
    for index_name, data in all_data.items():
        result_list = connect_mongo_insert.insert_recode_in_mongo(client, db_name, INDEX_SETTINGS[index_name]["collection_name"], data)
