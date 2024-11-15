from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
from pymongo import MongoClient
import time
import json
import requests

# MongoDB 연결
client = MongoClient('mongodb://192.168.0.63:27017/')
db = client['joesDB']
collection = db['tossStockComments']

stock_code = ['MSOX']  # 분석하고자 하는 종목 코드

def get_toss_comments(stock_code):
    driver = webdriver.Chrome()
    comments_by_date = {}  # 날짜별 댓글을 저장할 딕셔너리
    
    try:
        url = f"https://tossinvest.com/stocks/{stock_code}/community"
        driver.get(url)
        
        last_height = driver.execute_script("return document.body.scrollHeight")
        processed_comments = set()  # 이미 처리한 댓글 추적
        
        while True:
            wait = WebDriverWait(driver, 10)
            comments = wait.until(EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, "#stock-content > div > div > section > section > ul > div > div > article")
            ))
            
            # 새로운 댓글 처리
            new_comments_found = False
            for comment in comments:
                try:
                    # 댓글 내용과 날짜 추출
                    comment_text = comment.find_element(By.CSS_SELECTOR, "#stock-content > div > div > section > section > ul > div > div > article > div > a > span:nth-child(2) > span").text
                    date_element = comment.find_element(By.CSS_SELECTOR, "#stock-content > div > div > section > section > ul > div > div> article > div > header > div > label > time")
                    date_text = date_element.get_attribute("datetime").split('T')[0]  # YYYY-MM-DD 형식
                    
                    # 중복 체크를 위한 고유 식별자 생성
                    comment_id = f"{date_text}_{comment_text}"
                    
                    if comment_id not in processed_comments:
                        processed_comments.add(comment_id)
                        new_comments_found = True
                        
                        # 날짜별로 댓글 저장
                        if date_text not in comments_by_date:
                            comments_by_date[date_text] = []
                        comments_by_date[date_text].append(comment_text)
                        
                        print(f"날짜: {date_text}, 수집된 댓글 수: {len(processed_comments)}")
                
                except Exception as e:
                    print(f"댓글 처리 중 오류 발생: {e}")
                    continue
            
            # 스크롤 다운
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            
            new_height = driver.execute_script("return document.body.scrollHeight")
            
            # 더 이상 새로운 댓글이 없고 높이가 같으면 종료
            if not new_comments_found and new_height == last_height:
                print("모든 댓글을 수집했습니다.")
                break
                
            last_height = new_height
        
        return comments_by_date
        
    except Exception as e:
        print(f"에러 발생: {e}")
        return {}
        
    finally:
        driver.quit()

def analyze_sentiment(comment_text):
    uri = 'https://naveropenapi.apigw.ntruss.com/sentiment-analysis/v1/analyze'
    headers = {
        'X-NCP-APIGW-API-KEY-ID': '???',
        'X-NCP-APIGW-API-KEY': '???',
        'Content-Type': 'application/json'
    }
    body = {"content": comment_text}
    
    try:
        response = requests.post(url=uri, headers=headers, data=json.dumps(body))
        result = json.loads(response.text)
        return result
    except Exception as e:
        print(f"감정 분석 중 오류 발생: {e}")
        return None

def save_to_mongodb(stock_code, date, comment_text, sentiment_result):
    try:
        document = {
            'stock_code': stock_code,
            'date': date,
            'comment': comment_text,
            'sentiment': sentiment_result,
            'created_at': datetime.now()
        }
        result = collection.insert_one(document)
        print(f"MongoDB에 저장 완료 (ID: {result.inserted_id})")
    except Exception as e:
        print(f"DB 저장 중 오류 발생: {e}")

if __name__ == "__main__":
    for code in stock_code:
        print(f"\n{code} 종목의 댓글을 수집하고 분석합니다...")
        comments_by_date = get_toss_comments(code)
        
        for date in sorted(comments_by_date.keys()):
            print(f"\n날짜: {date} 처리 중...")
            for comment in comments_by_date[date]:
                # 감정 분석 수행
                sentiment_result = analyze_sentiment(comment)
                if sentiment_result:
                    # MongoDB에 저장
                    save_to_mongodb(code, date, comment, sentiment_result)
                time.sleep(1)  # API 호출 간격 조절
        
        print(f"{code} 종목 처리 완료")
        print("-" * 50)