from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import time
import json

stock_code = [
    'A005930'
    #A035720'
    #'007540'
    # 여기에 더 많은 종목 추가
]


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

def save_comments_to_file(stock_code, comments_by_date):
    # 현재 시간을 파일명에 포함
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"comments_{stock_code}_{timestamp}.json"
    
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(comments_by_date, f, ensure_ascii=False, indent=4)
        print(f"댓글이 {filename}에 저장되었습니다.")
    except Exception as e:
        print(f"파일 저장 중 오류 발생: {e}")

# 사용 예시
if __name__ == "__main__":
    for code in stock_code:
        print(f"\n{code} 종목의 댓글을 수집합니다...")
        comments_by_date = get_toss_comments(code)
        
        # 수집된 댓글 출력
        for date in sorted(comments_by_date.keys()):
            print(f"\n날짜: {date}")
            print(f"댓글 수: {len(comments_by_date[date])}")
        
        # 파일로 저장
        save_comments_to_file(code, comments_by_date)
        print("-" * 50)