# 공통 부분을 import 하여 구현
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pymongo import MongoClient
from commons.api_send_requester import ApiRequester
from commons.mongo_insert_recode import connect_mongo

ip_add = f'mongodb://localhost:27017/'
db_name = f'DB_SGMN' # db name 바꾸기
col_name = f'COLLECTION_INTERESTRATE' # collection name 바꾸기

class api_test_class:
    def api_test_func():
        # api
        base_url = f'https://kosis.kr/openapi/Param/statisticsParameterData.do'
        
        params={
                'method' : 'getList'
                ,'apiKey' : 'OGI2YzVmZmMyNjNiNjBmNjllN2E3MTkwN2RmMTk0Zjk='
                ,'itmId' : 'T1+'
                ,'objL1' : 'ALL'
                ,'objL2' : ''
                ,'objL3' : ''
                ,'objL4' : ''
                ,'objL5' : ''
                ,'objL6' : ''
                ,'objL7' : ''
                ,'objL8' : ''
                ,'format' : 'json'
                ,'jsonVD' : 'Y'
                ,'prdSe' : 'Y'
                ,'startPrdDe' : '1970'
                ,'endPrdDe' : '2025'
                ,'orgId' : '101'
                ,'tblId' : 'DT_2OEEO032'
                }

        result_geo = ApiRequester.send_api(base_url, params)
        return result_geo

if __name__ == "__main__":
    result_data = api_test_class.api_test_func()
    client = MongoClient(ip_add) # 관리 신경써야 함.
    result_list = connect_mongo.insert_recode_in_mongo(client, db_name, col_name, result_data)
    pass
