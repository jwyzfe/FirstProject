from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timedelta
import time
import json
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
import os
from pymongo import MongoClient
import pandas as pd

stock_code = ['041510']




# MongoDB 연결 설정
client = MongoClient('mongodb://localhost:27017/')
db = client['stock_db']
collection = db['toss_comments']

def save_comments(stock_code, comments_data):
    """댓글 데이터를 MongoDB에 저장하는 함수"""
    try:
        # 각 날짜별, 댓글별로 개별 도큐먼트 저장
        for date in comments_data.keys():
            for comment in comments_data[date]:
                document = {
                    "symbol": stock_code,
                    "date": date,
                    "comment": comment,
                    "updated_at": datetime.now()
                }
                collection.insert_one(document)
        
        print(f"{stock_code} 종목의 데이터가 MongoDB에 저장되었습니다.")
        
        # 저장된 데이터 요약 출력
        total_comments = sum(len(comments) for comments in comments_data.values())
        print(f"총 {len(comments_data)}개 날짜, {total_comments}개의 댓글이 저장됨")
        
        # 날짜별 댓글 수 출력
        for date in sorted(comments_data.keys()):
            comment_count = len(comments_data[date])
            print(f"{date}: {comment_count}개의 댓글")
            
    except Exception as e:
        print(f"MongoDB 저장 중 오류 발생: {e}")

class scrap_toss_comment:

    @classmethod
    def run_toss_comments(cls, stock_code_list):
        combined_df = pd.DataFrame()  # 초기 빈 DataFrame 생성
        
        for code in stock_code_list:
            df_comments = cls.get_toss_comments(code)  # 각 종목의 댓글을 DataFrame으로 가져옴
            
            # DataFrame이 비어있지 않은 경우에만 concat
            if not df_comments.empty:
                combined_df = pd.concat([combined_df, df_comments], ignore_index=True)
        
        return combined_df  # 최종 결과 반환


    @classmethod
    def get_toss_comments(cls, stock_code):
        # ChromeDriver 설정 부분 수정
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # GUI 없이 실행하는 옵션
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")

        # ChromeDriver 자동 설치 및 서비스 설정
        service = Service('/usr/bin/chromedriver')

        # ChromeDriver 실행
        driver = webdriver.Chrome(service=service, options=chrome_options)
        processed_comments = set()
        
        # DataFrame 초기화
        commentsForTwoDays = pd.DataFrame(columns=["datetime", "date", "symbol", "comment", "updated_at"])
        
        try:
            url = f"https://tossinvest.com/stocks/{stock_code}/community"
            print(f"symbol 1: {stock_code}")
            driver.get(url)
            
            try:
                wait = WebDriverWait(driver, 10)
                sort_button = wait.until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "#stock-content > div > div > section > section > section > button > span")
                ))
                
                if sort_button.text == '최신순':
                    sort_button.click()
                else:
                    sort_button.click()
                    time.sleep(1)
                
                time.sleep(2)
                
                last_height = driver.execute_script("return document.body.scrollHeight")
                duplicate_count = 0
                new_comments_count = 0
                
                while True:
                    comments = wait.until(EC.presence_of_all_elements_located(
                        (By.CSS_SELECTOR, "#stock-content > div > div > section > section > ul > div > div > article")
                    ))
                    
                    new_comments_found = False
                    for comment in comments:
                        try:
                            comment_text = comment.find_element(By.CSS_SELECTOR, 
                                "#stock-content > div > div > section > section > ul > div > div > article > div > a > span:nth-child(2) > span").text
                            date_element = comment.find_element(By.CSS_SELECTOR, 
                                "#stock-content > div > div > section > section > ul > div > div> article > div > header > div > label > time")
                            
                            # datetime 전체 값과 date 값 분리 저장
                            full_datetime = date_element.get_attribute("datetime")
                            date_only = full_datetime.split('T')[0]
                            
                            # 현재 시간을 updated_at으로 설정
                            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            
                            # 날짜 계산
                            comment_date = datetime.strptime(date_only, '%Y-%m-%d')
                            today = datetime.now().date()
                            two_days_ago = today - timedelta(days=2)
                            
                            if comment_date.date() < two_days_ago:
                                print("2일 이전의 댓글에 도달하여 수집을 종료합니다.")
                                return commentsForTwoDays
                            
                            comment_id = f"{date_only}_{comment_text}"
                            
                            if comment_id in processed_comments:
                                duplicate_count += 1
                                continue
                            
                            processed_comments.add(comment_id)
                            new_comments_found = True
                            new_comments_count += 1
                            
                            # DataFrame에 댓글 추가
                            comment_row = {
                                "datetime": full_datetime,     # 시간 포함
                                "date": date_only,            # 날짜만
                                "symbol": stock_code,
                                "comment": comment_text,
                                "updated_at": current_time    # 수집 시간 추가
                            }
                            
                            # DataFrame에 행 추가 (concat 사용)
                            commentsForTwoDays = pd.concat([commentsForTwoDays, pd.DataFrame([comment_row])], ignore_index=True)
                            
                            print(f"날짜: {date_only}, 새로운 댓글 수집 (새로 추가된 댓글: {new_comments_count}개)")
                        
                        except Exception as e:
                            print(f"댓글 처리 중 오류 발생: {e}")
                            continue
                    
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(2)
                    
                    new_height = driver.execute_script("return document.body.scrollHeight")
                    
                    if not new_comments_found and new_height == last_height:
                        print(f"수집 완료! 새로 추가된 댓글: {new_comments_count}개, 중복 제외된 댓글: {duplicate_count}개")
                        break
                        
                    last_height = new_height
                    
            except Exception as e:
                print(f"최신순 정렬 처리 중 오류 발생: {e}")
            
            return commentsForTwoDays  # DataFrame 반환

        except Exception as e:
            print(f"에러 발생: {e}")
            return pd.DataFrame()  # 빈 DataFrame 반환
            
        finally:
            driver.quit()

# 메인 실행 부분
if __name__ == "__main__":
    for code in stock_code:
        print(f"\n{code} 종목의 댓글을 수집합니다...")
        comments_list = scrap_toss_comment.get_toss_comments(code)
        
        # 날짜별로 댓글을 그룹화하여 출력
        dates = set(comment["date"] for comment in comments_list)
        for date in sorted(dates):
            date_comments = [c for c in comments_list if c["date"] == date]
            print(f"\n날짜: {date}")
            print(f"댓글 수: {len(date_comments)}")
        
        print("-" * 50)