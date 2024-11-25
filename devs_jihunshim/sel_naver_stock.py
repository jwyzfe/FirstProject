from selenium import webdriver
import time
from selenium.webdriver import ActionChains, Keys
from selenium.webdriver.common.by import By

# 셀레늄 공통 부분
opt = webdriver.ChromeOptions()
opt.add_experimental_option("detach", True)
browser = webdriver.Chrome(options=opt)

# 기업 목록 URL
company_urls = [
    "https://m.stock.naver.com/worldstock/stock/TSLA.O/discuss",
    "https://m.stock.naver.com/worldstock/stock/NVDA.O/discuss",
    "https://m.stock.naver.com/worldstock/stock/AAPL.O/discuss",
    "https://m.stock.naver.com/worldstock/stock/MSFT.O/discuss",
    "https://m.stock.naver.com/worldstock/stock/AMZN.O/discuss",
    "https://m.stock.naver.com/worldstock/stock/GOOG.O/discuss",
    "https://m.stock.naver.com/worldstock/stock/META.O/discuss",
    "https://m.stock.naver.com/worldstock/stock/BRKb/discuss",
    "https://m.stock.naver.com/worldstock/stock/AVGO.O/discuss",
    "https://m.stock.naver.com/worldstock/stock/WMT/discuss",
    "https://m.stock.naver.com/worldstock/stock/JPM/discuss",
    "https://m.stock.naver.com/worldstock/stock/LLY/discuss",
    "https://m.stock.naver.com/worldstock/stock/V/discuss",
    "https://m.stock.naver.com/worldstock/stock/UNH/discuss",
    "https://m.stock.naver.com/worldstock/stock/XOM/discuss",
    "https://m.stock.naver.com/worldstock/stock/ORCL.K/discuss",
    "https://m.stock.naver.com/worldstock/stock/MA/discuss",
    "https://m.stock.naver.com/worldstock/stock/COST.O/discuss",
    "https://m.stock.naver.com/worldstock/stock/HD/discuss",
    "https://m.stock.naver.com/worldstock/stock/PG/discuss",
    "https://m.stock.naver.com/worldstock/stock/NFLX.O/discuss",
    "https://m.stock.naver.com/worldstock/stock/JNJ/discuss",
    "https://m.stock.naver.com/worldstock/stock/BAC/discuss",
    "https://m.stock.naver.com/worldstock/stock/CRM/discuss",
    "https://m.stock.naver.com/worldstock/stock/ABBV.K/discuss"
]

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
            more_button = browser.find_element(By.CSS_SELECTOR, "#content > div.VMore_article__DPpKw.pagingMore > a")
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
        
    comments = browser.find_elements(By.CSS_SELECTOR, "a.DiscussListItem_link__pxeaR")
    print(f"총 댓글 수 ({url}) : {len(comments)}")

    for comment in comments : 
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
            like = comment.find_element(By.CSS_SELECTOR,"a.DiscussListItem_link__pxeaR > div.DiscussListItem_recomm__PyUH8 > span:nth-child(1)").text.strip()
        except Exception:
            like = "0"
        
        try:
            dislike = comment.find_element(By.CSS_SELECTOR,"a.DiscussListItem_link__pxeaR > div.DiscussListItem_recomm__PyUH8 > span:nth-child(2)").text.strip()
        except Exception:
            dislike = "0"
        
        try:
            view = comment.find_element(By.CSS_SELECTOR,"a.DiscussListItem_link__pxeaR > div.DiscussListItem_info__B716h > span:nth-child(3)").text.strip()
        except Exception:
            view = "0"
            
        print(f"제목 : {title}")
        print(f"날짜 : {date}")
        print(f"내용 : {contents}")
        print(f"{like}")
        print(f"{dislike}")
        print(f": {view}")
        print("-" * 50)

    print("\n" + "="*50 + "\n")
    
    
    '''
    
     if client is None:
            client = MongoClient(ip_add)
        result_list = connect_mongo_insert.insert_recode_in_mongo(client, db_name, col_name_dest, result_data)
    
    # # 직접 만든 class나 func을 참조하려면 꼭 필요 => main processor가 경로를 잘 몰라서 알려주어야함.
# import sys
# import os
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                
# from commons.mongo_insert_recode import connect_mongo as connect_mongo_insert

    '''