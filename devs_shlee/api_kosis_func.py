
# 공통 부분을 import 하여 구현
# 직접 만든 class나 func을 참조하려면 꼭 필요 => main processor가 경로를 잘 몰라서 알려주어야함.
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                
from commons.config_reader import read_config # config read 용    
from commons.api_send_requester import ApiRequester
from commons.mongo_insert_recode import connect_mongo as connect_mongo_insert

from pymongo import MongoClient

class api_test_class:

    '''
    기업경기조사(실적)
    https://kosis.kr/openapi/Param/statisticsParameterData.do
    ?method=getList&apiKey=인증키없음&itmId=13103134673999+&objL1=ALL&objL2=ALL&objL3=&objL4=&objL5=&objL6=&objL7=&objL8=&format=json&jsonVD=Y&prdSe=M&newEstPrdCnt=3&
    orgId=301&tblId=DT_512Y013
    https://kosis.kr/openapi/Param/statisticsParameterData.do
    ?method=getList&apiKey=인증키없음&itmId=13103134673999+&objL1=ALL&objL2=ALL&objL3=&objL4=&objL5=&objL6=&objL7=&objL8=&format=json&jsonVD=Y&prdSe=M&newEstPrdCnt=3&
    outputFields=ORG_ID+TBL_ID+TBL_NM+OBJ_ID+OBJ_NM+OBJ_NM_ENG+NM+NM_ENG+ITM_ID+ITM_NM+ITM_NM_ENG+UNIT_NM+UNIT_NM_ENG+PRD_SE+PRD_DE+LST_CHN_DE+&
    orgId=301&tblId=DT_512Y013

    기업경기조사(전망)
    https://kosis.kr/openapi/Param/statisticsParameterData.do
    ?method=getList&apiKey=인증키없음&itmId=13103134488999+&objL1=ALL&objL2=ALL&objL3=&objL4=&objL5=&objL6=&objL7=&objL8=&format=json&jsonVD=Y&prdSe=M&newEstPrdCnt=3&
    orgId=301&tblId=DT_512Y014

    
    https://kosis.kr/openapi/Param/statisticsParameterData.do
    ?method=getList&apiKey=인증키없음&itmId=13103134712999+&objL1=ALL&objL2=ALL&objL3=&objL4=&objL5=&objL6=&objL7=&objL8=&format=json&jsonVD=Y&prdSe=M&newEstPrdCnt=3&
    orgId=301&tblId=DT_512Y015

    경제심리지수
    https://kosis.kr/openapi/Param/statisticsParameterData.do
    ?method=getList&apiKey=인증키없음&itmId=13103134473999+&objL1=ALL&objL2=&objL3=&objL4=&objL5=&objL6=&objL7=&objL8=&format=json&jsonVD=Y&prdSe=M&newEstPrdCnt=3&
    orgId=301&tblId=DT_513Y001

    경기종합지수
    https://kosis.kr/openapi/Param/statisticsParameterData.do
    ?method=getList&apiKey=인증키없음&itmId=T1+&objL1=ALL&objL2=&objL3=&objL4=&objL5=&objL6=&objL7=&objL8=&format=json&jsonVD=Y&prdSe=M&newEstPrdCnt=3&
    orgId=101&tblId=DT_1C8015

    
    
    https://kosis.kr/openapi/Param/statisticsParameterData.do
    ?method=getList&apiKey=인증키없음&itmId=13103135843999+&objL1=ALL&objL2=ALL&objL3=&objL4=&objL5=&objL6=&objL7=&objL8=&format=json&jsonVD=Y&prdSe=M&newEstPrdCnt=3&
    orgId=301&tblId=DT_512Y021
    '''

    '''
    1. url 변경이 있을지
    2. 파라미터는 당연 변경 됨 유의할 것
    2. apikey 도 받아와야함.
    '''
    def api_test_func():
        # api

        base_url = f' https://kosis.kr/openapi/Param/statisticsParameterData.do'
        config = read_config()
        apiKey = config['kosis']['api_key']
        params = {
            "method": "getList",
            "apiKey": "Mzg2OGUwZTA2NzliMWZjMDM4MDhhZmVkYjY4MzJlODA=",
            "itmId": "13103134673999",
            "objL1": "ALL",
            "objL2": "ALL",
            "objL3": "",
            "objL4": "",
            "objL5": "",
            "objL6": "",
            "objL7": "",
            "objL8": "",
            "format": "json",
            "jsonVD": "Y",
            "prdSe": "M",
            "newEstPrdCnt": 3,
            "orgId": 301,
            "tblId": "DT_512Y013"
        }

        params = {
            "method": "getList",
            "apiKey": "Mzg2OGUwZTA2NzliMWZjMDM4MDhhZmVkYjY4MzJlODA=",
            "itmId": "T1",
            "objL1": "ALL",
            "objL2": "",
            "objL3": "",
            "objL4": "",
            "objL5": "",
            "objL6": "",
            "objL7": "",
            "objL8": "",
            "format": "json",
            "jsonVD": "Y",
            "prdSe": "M",
            "newEstPrdCnt": 3,
            "orgId": 101,
            "tblId": "DT_1C8015"
        }

        params = {
            "method": "getList",
            "apiKey": "Mzg2OGUwZTA2NzliMWZjMDM4MDhhZmVkYjY4MzJlODA=",
            "itmId": "T10",
            "objL1": "ALL",
            "objL2": "",
            "objL3": "",
            "objL4": "",
            "objL5": "",
            "objL6": "",
            "objL7": "",
            "objL8": "",
            "format": "json",
            "jsonVD": "Y",
            "prdSe": "M",
            "startPrdDe": "197001",
            "endPrdDe": "202410",
            "orgId": 101,
            "tblId": "DT_1C8016"
        }

        result_data = ApiRequester.send_api(base_url, params)
        return result_data

if __name__ == "__main__":

    config = read_config()
    ip_add = config['MongoDB_local']['ip_add']
    db_name = config['MongoDB_local']['db_name']
    client = MongoClient(ip_add)
    col_name = 'COL_TEST_02'

    result_data = api_test_class.api_test_func()
    result_list = connect_mongo_insert.insert_recode_in_mongo(client, db_name, col_name, result_data)

    pass