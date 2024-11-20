from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timedelta
import time
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from pymongo import MongoClient
import pytz

# Chrome 브라우저 설정
chrome_options = Options()
# chrome_options.add_argument("--start-maximized")  # 브라우저 창 최대화
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# ChromeDriver 자동 설치 및 서비스 설정
service = Service(ChromeDriverManager().install())

# MongoDB 연결 설정
client = MongoClient('mongodb://localhost:27017/')
db = client['stock_db']
collection = db['toss_comments']

class UpdateDailyTossComments:
    # def __init__(self):
    #     """
    #     MongoDB 연결 초기화
    #     로컬 테스트를 위해 localhost:27017 사용
    #     """
    #     try:
    #         self.client = MongoClient('mongodb://localhost:27017/')
    #         self.db = self.client['stock_db']
    #         self.collection = self.db['toss_comments']
    #         print("MongoDB 연결 성공")
    #     except Exception as e:
    #         print(f"MongoDB 연결 실패: {str(e)}")

    def get_latest_comment_date(self, symbol):
        """
        특정 심볼의 가장 최근 댓글 날짜를 조회
        Args:
            symbol (str): 주식 심볼
        Returns:
            datetime: 가장 최근 댓글 날짜, 없으면 None
        """
        try:
            latest_comment = self.collection.find_one(
                {'symbol': symbol},
                sort=[('date', -1)]
            )
            if latest_comment:
                # date가 문자열인 경우 datetime으로 변환
                if isinstance(latest_comment['date'], str):
                    latest_date = datetime.strptime(latest_comment['date'], '%Y.%m.%d')
                else:
                    latest_date = latest_comment['date']
                print(f"최근 댓글 날짜: {latest_date.strftime('%Y.%m.%d')}")
                return latest_date
            return None
        except Exception as e:
            print(f"최근 댓글 조회 실패: {str(e)}")
            return None

    def get_toss_comments(self, symbol):
        driver = webdriver.Chrome(service=service, options=chrome_options)
        comments_by_date = {}
        continue_scraping = True
        found_duplicate = False
        target_date = None
        
        try:
            url = f"https://tossinvest.com/stocks/{symbol}/community"
            driver.get(url)
            print(f"{symbol} 페이지 접속 성공")
            
            # MongoDB에서 해당 심볼의 모든 댓글 가져오기
            existing_comments = set()
            for comment in self.collection.find({'symbol': symbol}):
                comment_key = f"{comment['date'].strftime('%Y.%m.%d')}_{comment['comment']}"
                existing_comments.add(comment_key)
            print(f"DB에서 {len(existing_comments)}개의 기존 댓글 로드")
            
            # 정렬 버튼의 텍스트 확인
            sort_button = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#stock-content > div > div > section > section > section > button > span"))
            )
            
            if "최신순" not in sort_button.text:
                sort_button.click()
                time.sleep(1)

            while continue_scraping:
                comment_elements = driver.find_elements(By.CSS_SELECTOR, "#stock-content > div > div > section > section > ul > div > div > article")
                
                for comment in comment_elements:
                    try:
                        date_text = comment.find_element(By.CSS_SELECTOR, "#stock-content > div > div > section > section > ul > div > div> article > div > header > div > label > time").text
                        comment_text = comment.find_element(By.CSS_SELECTOR, "#stock-content > div > div > section > section > ul > div > div > article > div > a > span:nth-child(2) > span").text
                        
                        if "." not in date_text:
                            continue
                            
                        current_date = datetime.strptime(date_text, '%Y.%m.%d')
                        
                        # DB에 있는 댓글인지 확인
                        comment_key = f"{date_text}_{comment_text}"
                        if not found_duplicate and comment_key in existing_comments:
                            found_duplicate = True
                            target_date = (current_date - timedelta(days=2))
                            print(f"중복 발견: {date_text}, 목표 수집 시작일: {target_date.strftime('%Y.%m.%d')}")
                            break
                        
                        # 댓글 저장
                        if date_text not in comments_by_date:
                            comments_by_date[date_text] = []
                        
                        comments_by_date[date_text].append({
                            'symbol': symbol,
                            'date': current_date,
                            'comment': comment_text
                        })
                            
                    except Exception as e:
                        print(f"댓글 처리 중 오류 발생: {e}")
                        continue
                
                # 중복을 찾았으면 스크래핑 중단
                if found_duplicate:
                    break
                    
                # 중복을 찾지 않은 경우에만 스크롤
                last_height = driver.execute_script("return document.documentElement.scrollHeight")
                driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
                time.sleep(2)
                new_height = driver.execute_script("return document.documentElement.scrollHeight")
                
                if new_height == last_height:
                    break
            
            # 첫 페이지로 돌아가서 현재부터 target_date까지의 댓글 수집
            if target_date:
                comments_by_date.clear()  # 기존 수집 데이터 초기화
                driver.get(url)
                time.sleep(2)
                
                while True:
                    comment_elements = driver.find_elements(By.CSS_SELECTOR, "#stock-content > div > div > section > section > ul > div > div > article")
                    
                    for comment in comment_elements:
                        try:
                            date_text = comment.find_element(By.CSS_SELECTOR, "#stock-content > div > div > section > section > ul > div > div> article > div > header > div > label > time").text
                            comment_text = comment.find_element(By.CSS_SELECTOR, "#stock-content > div > div > section > section > ul > div > div > article > div > a > span:nth-child(2) > span").text
                            
                            if "." not in date_text:
                                continue
                                
                            current_date = datetime.strptime(date_text, '%Y.%m.%d')
                            
                            # target_date 이전이면 종료
                            if current_date < target_date:
                                break
                            
                            if date_text not in comments_by_date:
                                comments_by_date[date_text] = []
                            
                            comments_by_date[date_text].append({
                                'symbol': symbol,
                                'date': current_date,
                                'comment': comment_text
                            })
                            
                        except Exception as e:
                            print(f"댓글 처리 중 오류 발생: {e}")
                            continue
                    
                    last_height = driver.execute_script("return document.documentElement.scrollHeight")
                    driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
                    time.sleep(2)
                    new_height = driver.execute_script("return document.documentElement.scrollHeight")
                    
                    if new_height == last_height or current_date < target_date:
                        break
            
            # 수집된 모든 댓글을 리스트로 변환
            all_comments = []
            for date_comments in comments_by_date.values():
                all_comments.extend(date_comments)
                
            print(f"총 {len(all_comments)}개의 댓글 수집 완료")
            return all_comments
            
        except Exception as e:
            print(f"에러 발생: {e}")
            return []
        
        finally:
            driver.quit()

    def save_to_mongodb(self, comments):
        """
        수집된 댓글을 MongoDB에 저장
        Args:
            comments (list): 저장할 댓글 데이터 리스트
        """
        if comments:
            try:
                self.collection.insert_many(comments)
                print(f"MongoDB 저장 성공: {len(comments)}개 댓글")
            except Exception as e:
                print(f"MongoDB 저장 실패: {str(e)}")

    def update_daily_comments(self, symbol_list):
        """
        심볼 리스트의 댓글을 수집하고 저장
        Args:
            symbol_list (list): 수집할 주식 심볼 리스트
        """
        for symbol in symbol_list:
            print(f"\n=== {symbol} 종목 처리 시작 ===")
            comments = self.get_toss_comments(symbol)
            self.save_to_mongodb(comments)
            print(f"=== {symbol} 종목 처리 완료 ===\n")

# 테스트 실행용 함수
def test_run():
    """
    테스트용 실행 함수
    단일 심볼에 대해 즉시 실행
    """
    symbol_list = ['007540']  # 테스트용 심볼
    updater = UpdateDailyTossComments()
    updater.update_daily_comments(symbol_list)

if __name__ == "__main__":
    test_run()

            