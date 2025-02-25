from selenium import webdriver
from selenium.webdriver import ActionChains, Keys
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service 
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options
import platform

import time

import sys
import os
import pandas as pd
# 현재 파일의 두 단계 상위 디렉토리(FirstProject)를 path에 추가.
current_dir = os.path.dirname(os.path.abspath(__file__))  # /manage
parent_dir = os.path.dirname(current_dir)  # /schedulers
project_dir = os.path.dirname(parent_dir)  # /FirstProject
sys.path.append(project_dir)

from pymongo import MongoClient
from commons.mongo_insert_recode import connect_mongo as connect_mongo_insert


# 셀레늄 공통 부분
# opt = webdriver.ChromeOptions()
# opt.add_experimental_option("detach", True)
# browser = webdriver.Chrome(options=opt)

class all_print:

    @staticmethod
    def get_browser():
        options = Options()
        
        # 운영체제 확인
        current_os = platform.system().lower()
        
        if current_os == 'linux':
            # Linux 환경 설정
            options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")

            # 추가 옵션들
            options.add_argument("--window-size=1920,1080")  # 충분한 뷰포트 크기
            options.add_argument("--disable-gpu")
            options.add_argument("--enable-javascript")
            browser = webdriver.Chrome(service=Service('/usr/bin/chromedriver'), options=options)
        else:
            # Windows 또는 다른 환경 설정
            webdriver_manager_directory = ChromeDriverManager().install()
            browser = webdriver.Chrome(service=Service(webdriver_manager_directory), options=options)
        
        return browser


    def reply_list(browser, company_urls):
        
        # 기업 목록 URL
        naver_stock_reply=[]
        for url in company_urls:
            browser.get(url)
            print(f"현재 URL : {url}")
            scroll_count = 0
            max_count = 2
            prev_height = 0
            same_num_cnt = 0

            while scroll_count < max_count :
                ActionChains(browser).key_down(Keys.PAGE_DOWN).perform()
                cur_height = browser.execute_script("return document.documentElement.scrollTop")
                time.sleep(1)
                scroll_count += 1
                
                try:
                    # 더보기 클릭 명령어(?)
                    more_button = browser.find_element(By.CSS_SELECTOR, "#content > div.VMore_article__DPpKw.pagingMore > a") #content > div.VMore_article__DPpKw.pagingMore > a
                    more_button.click()
                    time.sleep(1)
                except Exception as e:
                    # "더보기" 버튼이 없을 경우 예외를 무시하고 진행
                    pass
                        
                if prev_height == cur_height:
                    same_num_cnt += 1
                else:
                    same_num_cnt = 0
                if same_num_cnt == 20:    
                    break
                prev_height = cur_height 
                
            comments = browser.find_elements(By.CSS_SELECTOR, "a.DiscussListItem_link__pxeaR") #a.DiscussListItem_link__pxeaR
            print(f"총 댓글 수 ({url}) : {len(comments)}")

            for comment in comments : #
                try:
                    title = comment.find_element(By.CSS_SELECTOR,"a.DiscussListItem_link__pxeaR > strong").text.strip()
                except Exception:
                    title = "제목 없음"
                    
                try:
                    date = comment.find_element(By.CSS_SELECTOR,"a.DiscussListItem_link__pxeaR > div.DiscussListItem_info__B716h > span:nth-child(2)").text.strip()
                except Exception:
                    date = "날짜없음"
                
                try:
                    contents = comment.find_element(By.CSS_SELECTOR,"a.DiscussListItem_link__pxeaR > p").text.strip()
                except Exception:
                    contents = "내용없음"
                
                try:
                    like = comment.find_element(By.CSS_SELECTOR,"a.DiscussListItem_link__pxeaR > div.DiscussListItem_recomm__PyUH8 > span:nth-child(1)").text[3:].strip()
                    # like = comment.find_element(By.CSS_SELECTOR,"a.DiscussListItem_link__pxeaR > div.DiscussListItem_recomm__PyUH8 > span:nth-child(1)")
                except Exception:
                    like = "0"
                    
                try:
                    dislike = comment.find_element(By.CSS_SELECTOR,"a.DiscussListItem_link__pxeaR > div.DiscussListItem_recomm__PyUH8 > span:nth-child(2)").text[4:].strip()
                except Exception:
                    dislike = "0"
                
                try:
                    view = comment.find_element(By.CSS_SELECTOR,"a.DiscussListItem_link__pxeaR > div.DiscussListItem_info__B716h > span:nth-child(3)").text[4:].strip()
                                                                # a.DiscussListItem_link__pxeaR > div.DiscussListItem_info__B716h > span:nth-child(3)
                except Exception:
                    view = "0"
                    
                all_print = {
                            "제목" : title
                            ,"날짜" : date
                            ,"내용" : contents
                            ,"추천" : like
                            ,"비추천" : dislike
                            ,"조회수" : view
                            }
                # print(all_print)
                naver_stock_reply.append(all_print)
        return naver_stock_reply
    
    def get_symbol_list_to_reply(symbol_list):
        browser = all_print.get_browser()
        transformed_data = []
        
        for symbol in symbol_list:
            if symbol.isdigit():
                base_url = f"https://m.stock.naver.com/domestic/stock/{symbol}/discuss"
            else:
                base_url = f"https://m.stock.naver.com/worldstock/stock/{symbol}/discuss"
                
            raw_result_data = all_print.reply_list(browser, [base_url])
            
            # 현재 symbol에 대한 결과 데이터 변환
            for item in raw_result_data:
                transformed_item = {
                    "SYMBOL": symbol,
                    "TITLE": item["제목"],
                    "DATE": item["날짜"],
                    "CONTENT": item["내용"],
                    "LIKE": item["추천"],
                    "DISLIKE": item["비추천"],
                    "VIEW": item["조회수"]
                }
                transformed_data.append(transformed_item)
        
        return transformed_data




if __name__=='__main__':
    company_urls = [
            "https://m.stock.naver.com/worldstock/stock/TSLA.O/discuss",
            # "https://m.stock.naver.com/worldstock/stock/NVDA.O/discuss",
            # "https://m.stock.naver.com/worldstock/stock/AAPL.O/discuss",
            # "https://m.stock.naver.com/worldstock/stock/MSFT.O/discuss",
            # "https://m.stock.naver.com/worldstock/stock/AMZN.O/discuss",
            # "https://m.stock.naver.com/worldstock/stock/GOOG.O/discuss",
            # "https://m.stock.naver.com/worldstock/stock/META.O/discuss",
            # "https://m.stock.naver.com/worldstock/stock/BRKb/discuss",
            # "https://m.stock.naver.com/worldstock/stock/AVGO.O/discuss",
            # "https://m.stock.naver.com/worldstock/stock/WMT/discuss",
            # "https://m.stock.naver.com/worldstock/stock/JPM/discuss",
            # "https://m.stock.naver.com/worldstock/stock/LLY/discuss",
            # "https://m.stock.naver.com/worldstock/stock/V/discuss",
            # "https://m.stock.naver.com/worldstock/stock/UNH/discuss",
            # "https://m.stock.naver.com/worldstock/stock/XOM/discuss",
            # "https://m.stock.naver.com/worldstock/stock/ORCL.K/discuss",
            # "https://m.stock.naver.com/worldstock/stock/MA/discuss",
            # "https://m.stock.naver.com/worldstock/stock/COST.O/discuss",
            # "https://m.stock.naver.com/worldstock/stock/HD/discuss",
            # "https://m.stock.naver.com/worldstock/stock/PG/discuss",
            # "https://m.stock.naver.com/worldstock/stock/NFLX.O/discuss",
            # "https://m.stock.naver.com/worldstock/stock/JNJ/discuss",
            # "https://m.stock.naver.com/worldstock/stock/BAC/discuss",
            # "https://m.stock.naver.com/worldstock/stock/CRM/discuss",
            # "https://m.stock.naver.com/worldstock/stock/ABBV.K/discuss"
        ]
    
    
    browser = all_print.get_browser()
    # input_list = all_print.reply_list(browser, company_urls)

    symbols = [ 'TSLA.O', '096775' ]

    input_list = all_print.get_symbol_list_to_reply(symbols)
    # 스케쥴러 등록 
    # mongodb 가져올 수 있도록
    # ip_add = f'mongodb://192.168.0.50:27017/'
    ip_add = f'mongodb://192.168.0.91:27017/'
    db_name = f'DB_SGMN' # db name 바꾸기
    col_name = f'COL_NAVER_STOCK_REPLY' # collection name 바꾸기
    
    # MongoDB 서버에 연결
    client = MongoClient(ip_add)
    result_list = connect_mongo_insert.insert_recode_in_mongo(client, db_name, col_name, input_list)