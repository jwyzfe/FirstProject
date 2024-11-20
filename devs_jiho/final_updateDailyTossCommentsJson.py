from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import time
import json
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
import os

stock_code = ["BKKT"]


# ChromeDriver 설정 부분 수정
chrome_options = Options()
# chrome_options.add_argument("--headless")  # GUI 없이 실행하는 옵션 제거
chrome_options.add_argument("--start-maximized")  # 브라우저 창 최대화
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# ChromeDriver 자동 설치 및 서비스 설정
service = Service(ChromeDriverManager().install())

# ChromeDriver 실행
browser = webdriver.Chrome(service=service, options=chrome_options)


def load_existing_comments(stock_code):
    """기존 댓글 데이터를 로드하는 함수"""
    filename = f"comments_{stock_code}.json"
    if os.path.exists(filename):
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
                print(f"기존 데이터 파일 로드: {filename}")
                return existing_data
        except Exception as e:
            print(f"기존 데이터 로드 중 오류 발생: {e}")
            return {}
    else:
        print(f"새로운 데이터 파일이 생성됩니다: {filename}")
        return {}

def save_comments(stock_code, comments_data):
    """댓글 데이터를 날짜순으로 정렬하여 저장하는 함수"""
    filename = f"comments_{stock_code}.json"
    try:
        # 날짜를 기준으로 정렬
        sorted_comments = {}
        for date in sorted(comments_data.keys(), reverse=True):  # 최신 날짜순으로 정렬
            sorted_comments[date] = comments_data[date]
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(sorted_comments, f, ensure_ascii=False, indent=4)
        print(f"데이터가 {filename}에 날짜순으로 저장되었습니다.")
        
        # 저장된 데이터 요약 출력
        total_comments = sum(len(comments) for comments in sorted_comments.values())
        print(f"총 {len(sorted_comments)}개 날짜, {total_comments}개의 댓글이 저장됨")
        
        # 날짜별 댓글 수 출력
        for date in sorted(sorted_comments.keys()):
            comment_count = len(sorted_comments[date])
            print(f"{date}: {comment_count}개의 댓글")
            
    except Exception as e:
        print(f"데이터 저장 중 오류 발생: {e}")

def get_toss_comments(stock_code):
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    # 기존 데이터 로드
    comments_by_date = load_existing_comments(stock_code)
    
    try:
        url = f"https://tossinvest.com/stocks/{stock_code}/community"
        driver.get(url)
        
        try:
            wait = WebDriverWait(driver, 10)
            # 최신순 정렬 버튼 클릭
            sort_button = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#stock-content > div > div > section > section > section > button > span")
            ))
            
            if sort_button.text != '최신순':
                sort_button.click()
                time.sleep(1)
            
            time.sleep(2)
            
            new_comments_count = 0
            should_stop = False
            
            while not should_stop:
                comments = wait.until(EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, "#stock-content > div > div > section > section > ul > div > div > article")
                ))
                
                for comment in comments:
                    try:
                        comment_text = comment.find_element(By.CSS_SELECTOR, 
                            "#stock-content > div > div > section > section > ul > div > div > article > div > a > span:nth-child(2) > span").text
                        date_element = comment.find_element(By.CSS_SELECTOR, 
                            "#stock-content > div > div > section > section > ul > div > div> article > div > header > div > label > time")
                        date_text = date_element.get_attribute("datetime").split('T')[0]
                        
                        # 해당 날짜의 댓글이 이미 존재하고, 댓글이 이미 있다면 수집 중단
                        if date_text in comments_by_date and comment_text in comments_by_date[date_text]:
                            print(f"기존 데이터 발견. 수집을 중단합니다. 새로 추가된 댓글: {new_comments_count}개")
                            should_stop = True
                            break
                        
                        # 새로운 댓글 추가
                        if date_text not in comments_by_date:
                            comments_by_date[date_text] = []
                        if comment_text not in comments_by_date[date_text]:
                            comments_by_date[date_text].append(comment_text)
                            new_comments_count += 1
                            print(f"날짜: {date_text}, 새로운 댓글 수집 (총 {new_comments_count}개)")
                    
                    except Exception as e:
                        print(f"댓글 처리 중 오류 발생: {e}")
                        continue
                
                if should_stop:
                    break
                    
                # 스크롤 다운
                last_height = driver.execute_script("return document.body.scrollHeight")
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                new_height = driver.execute_script("return document.body.scrollHeight")
                
                if new_height == last_height:
                    print("모든 새로운 댓글을 수집했습니다.")
                    break
            
            # 새로운 댓글이 있을 경우에만 파일 저장
            if new_comments_count > 0:
                save_comments(stock_code, comments_by_date)
            else:
                print("새로운 댓글이 없습니다.")
                
        except Exception as e:
            print(f"최신순 정렬 처리 중 오류 발생: {e}")
        
        return comments_by_date
        
    except Exception as e:
        print(f"에러 발생: {e}")
        return {}
        
    finally:
        driver.quit()

# 메인 실행 부분
if __name__ == "__main__":
    for code in stock_code:
        print(f"\n{code} 종목의 댓글을 수집합니다...")
        comments_by_date = get_toss_comments(code)
        
        # 수집된 댓글 출력
        for date in sorted(comments_by_date.keys()):
            print(f"\n날짜: {date}")
            print(f"댓글 수: {len(comments_by_date[date])}")
        
        print("-" * 50)

