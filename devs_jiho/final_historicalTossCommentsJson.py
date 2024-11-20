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

stock_code = ["MSOS"]


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
    processed_comments = set()  # 중복 체크를 위한 set
    
    # 기존 데이터 로드
    comments_by_date = load_existing_comments(stock_code)
    
    # 기존 데이터에서 중복 체크용 set 구성
    for date, comments in comments_by_date.items():
        for comment in comments:
            processed_comments.add(f"{date}_{comment}")
    
    print(f"기존 데이터에서 {len(processed_comments)}개의 댓글 로드 완료")
    
    try:
        url = f"https://tossinvest.com/stocks/{stock_code}/community"
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
                        date_text = date_element.get_attribute("datetime").split('T')[0]
                        
                        comment_id = f"{date_text}_{comment_text}"
                        
                        if comment_id in processed_comments:
                            duplicate_count += 1
                            continue
                        
                        processed_comments.add(comment_id)
                        new_comments_found = True
                        new_comments_count += 1
                        
                        # 날짜별로 댓글 저장
                        if date_text not in comments_by_date:
                            comments_by_date[date_text] = []
                        if comment_text not in comments_by_date[date_text]:  # 같은 날짜 내 중복 체크
                            comments_by_date[date_text].append(comment_text)
                        
                        print(f"날짜: {date_text}, 새로운 댓글 수집 (새로 추가된 댓글: {new_comments_count}개)")
                    
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
                
            # 새로운 댓글이 있을 경우에만 파일 저장
            if new_comments_count > 0:
                save_comments(stock_code, comments_by_date)
            else:
                print("새로운 댓글이 없어 파일을 업데이트하지 않았습니다.")
                
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