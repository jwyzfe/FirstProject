
# 스크래퍼 import 
from apscheduler.schedulers.background import BackgroundScheduler
# mongo DB 동작
from pymongo import MongoClient
# time.sleep
import time

# selenium driver 
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService

# 직접 만든 class 
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                
from commons.mongo_insert_recode import connect_mongo
from commons.api_send_requester import ApiRequester
from commons.templates.sel_iframe_courtauction import iframe_test
from commons.templates.bs4_do_scrapping import bs4_scrapping
from commons.mongo_find_recode import connect_mongo as mongo_find_recode

# 직접 구현한 부분을 import 해서 scheduler에 등록
from devs.api_test_class import api_test_class

def api_test_func():
    # api
    city_list = ['도쿄','괌','모나코']
    key_list = ['lat', 'lon']
    pub_key = '39fb7b1c6d4e11e7483aabcb737ce7b0'
    for city in city_list:
        base_url = f'https://api.openweathermap.org/geo/1.0/direct'
        
        params={}
        params['q'] = city
        params['appid'] = pub_key

        result_geo = ApiRequester.send_api(base_url, params, key_list)

        base_url = f'https://api.openweathermap.org/data/2.5/forecast'
        
        params_w = {}
        for geo in result_geo:
            for key in key_list:
                params_w[key] = geo[key]
        params_w['appid'] = pub_key
        result_cont = ApiRequester.send_api(base_url, params_w)

        print(result_cont)

# common 에 넣을 예정
def register_job_with_mongo(client, ip_add, db_name, col_name, func, insert_data):

    try:
        result_data = func(*insert_data)
        if client is None:
            client = MongoClient(ip_add) # 관리 신경써야 함.
        result_list = connect_mongo.insert_recode_in_mongo(client, db_name, col_name, result_data)       # print(f'insert id list count : {len(result_list.inserted_ids)}')
    except Exception as e :
        print(e)
        client.close()

    return 

def run():

    # 스케쥴러 등록 
    # mongodb 가져올 수 있도록
    ip_add = f'mongodb://192.168.0.48:27017/'
    db_name = f'db_name' # db name 바꾸기
    col_name = f'collection_jihunshim' # collection name 바꾸기

    # MongoDB 서버에 연결 
    client = MongoClient(ip_add) 

            # 2. ready 상태이면서 아직 완료되지 않은(ref_id가 finished_ref_ids에 없는) 레코드를 찾습니다
    ready_records = mongo_find_recode.get_records_dataframe(
            client, 
            db_name, 
            col_name, 
            {"iswork": "ready"}
        ) 

    while True:

        pass
    
    return True


if __name__ == '__main__':
    run()
    pass
