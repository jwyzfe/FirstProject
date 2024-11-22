# 필요한 라이브러리들을 임포트
from selenium import webdriver                    # 웹 브라우저 자동화를 위한 라이브러리
from selenium.webdriver.common.by import By       # 웹 요소 찾기 위한 기능
from selenium.webdriver.support.ui import WebDriverWait    # 웹 페이지 로딩 대기를 위한 기능
from selenium.webdriver.support import expected_conditions as EC  # 특정 조건 충족 대기를 위한 기능
from datetime import datetime, timedelta          # 날짜 처리를 위한 기능
import time                                      # 시간 지연을 위한 기능
from selenium.webdriver.chrome.service import Service    # 크롬드라이버 서비스 설정
from webdriver_manager.chrome import ChromeDriverManager # 크롬드라이버 자동 관리
from selenium.webdriver.chrome.options import Options   # 크롬 브라우저 옵션 설정
from pymongo import MongoClient                  # MongoDB 연결을 위한 라이브러리
import pytz                                     # 시간대 처리를 위한 라이브러리

# 크롬 브라우저 설정
chrome_options = Options()                      # 크롬 옵션 객체 생성
chrome_options.add_argument("--no-sandbox")     # 샌드박스 비활성화 (보안 관련)
chrome_options.add_argument("--disable-dev-shm-usage")  # 공유 메모리 사용 비활성화

# ChromeDriver 설정
service = Service(ChromeDriverManager().install())    # 크롬드라이버 자동 설치 및 서비스 설정

# MongoDB 연결 설정
client = MongoClient('mongodb://192.168.0.46:27017/')    # MongoDB 서버 연결
db = client['stock_db']                              # 데이터베이스 선택
collection = db['toss_comments']                     # 컬렉션 선택

class UpdateDailyTossComments:                       # 토스 댓글 업데이트 클래스 정의
    def __init__(self):                             ## 클래스 초기화 메서드 이거 왜쓰는데?
        try:
            # 로컬 MongoDB 연결
            self.client = MongoClient('mongodb://localhost:27017/')
            self.db = self.client['stock_db']
            self.collection = self.db['toss_comments']
            print("MongoDB 연결 성공")
        except Exception as e:
            print(f"MongoDB 연결 실패: {str(e)}")
            raise

    def get_latest_comment_date(self, symbol):       # 최신 댓글 날짜 조회 메서드
        try:
            latest_comment = self.collection.find_one(    # 특정 심볼의 가장 최근 댓글 찾기
                {'symbol': symbol},
                sort=[('date', -1)]                      # 날짜 기준 내림차순 정렬
            )
            if latest_comment:
                # 날짜 형식 처리
                if isinstance(latest_comment['date'], str):    # 날짜가 문자열인 경우
                    latest_date = datetime.strptime(latest_comment['date'], '%Y-%m-%d')  # datetime으로 변환
                else:
                    latest_date = latest_comment['date']      # 이미 datetime인 경우 그대로 사용
                print(f"최근 댓글 날짜: {latest_date.strftime('%Y-%m-%d')}")
                return latest_date
            return None
        except Exception as e:
            print(f"최근 댓글 조회 실패: {str(e)}")
            return None
    
    def get_toss_comments(self, symbol):
        # 최신 댓글 날짜 확인
        latest_date = self.get_latest_comment_date(symbol)
        if latest_date:
            print(f"DB의 최신 댓글 날짜: {latest_date}")
            target_date = latest_date
        else:
            # DB에 데이터가 없으면 2일 전부터 수집
            target_date = datetime.now() - timedelta(days=2)
            print(f"DB에 데이터가 없어 {target_date.strftime('%Y-%m-%d')}부터 수집합니다.")

        driver = webdriver.Chrome(service=service, options=chrome_options)
        comments_by_date = {}
        
        try:
            url = f"https://tossinvest.com/stocks/{symbol}/community"
            driver.get(url)
            print(f"{symbol} 페이지 접속 성공")
            
            # 최신순 정렬 설정
            sort_button = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#stock-content > div > div > section > section > section > button > span"))
            )
            
            if "최신순" not in sort_button.text:
                sort_button.click()
                time.sleep(1)

            while True:
                comment_elements = driver.find_elements(By.CSS_SELECTOR, "#stock-content > div > div > section > section > ul > div > div > article")
                
                for comment in comment_elements:
                    try:
                        date_text = comment.find_element(By.CSS_SELECTOR, "#stock-content > div > div > section > section > ul > div > div> article > div > header > div > label > time").text
                        comment_text = comment.find_element(By.CSS_SELECTOR, "#stock-content > div > div > section > section > ul > div > div > article > div > a > span:nth-child(2) > span").text
                        # date_text = date_element.get_attribute("datetime").split('T')[0]
                        # 날짜 변환
                        current_date = datetime.now()
                        if "분 전" in date_text or "시간 전" in date_text:
                            current_date = datetime.now()
                        elif "일 전" in date_text:
                            days_ago = int(date_text.split("일")[0])
                            current_date = datetime.now() - timedelta(days=days_ago)
                        elif "년" in date_text and "월" in date_text and "일" in date_text:
                            year = int(date_text.split("년")[0])
                            month = int(date_text.split("월")[0].split("년")[1])
                            day = int(date_text.split("일")[0].split("월")[1])
                            current_date = datetime(year, month, day)
                        
                        # target_date보다 이전 댓글이면 스크래핑 중단
                        if current_date.date() <= target_date.date():
                            print("이미 수집된 날짜의 댓글입니다. 스크래핑을 중단합니다.")
                            return comments_by_date
                        
                        formatted_date = current_date.strftime('%Y-%m-%d')
                        
                        # 날짜별로 댓글 저장
                        if formatted_date not in comments_by_date:
                            comments_by_date[formatted_date] = []
                        
                        comments_by_date[formatted_date].append({
                            'symbol': symbol,
                            'date': current_date,
                            'comment': comment_text
                        })
                            
                    except Exception as e:
                        print(f"댓글 처리 중 오류 발생: {e}")
                        continue
                
                # 페이지 스크롤
                last_height = driver.execute_script("return document.documentElement.scrollHeight")
                driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
                time.sleep(2)
                new_height = driver.execute_script("return document.documentElement.scrollHeight")
                
                if new_height == last_height:
                    break

            # MongoDB에 저장
            total_comments = 0
            for date in comments_by_date:
                comments = comments_by_date[date]
                if comments:
                    try:
                        self.collection.insert_many(comments)
                        total_comments += len(comments)
                    except Exception as e:
                        print(f"댓글 저장 중 오류 발생: {e}")
                        continue
            
            print(f"총 {total_comments}개의 새로운 댓글이 저장되었습니다.")
            
        except Exception as e:
            print(f"댓글 수집 중 오류 발생: {e}")
            
        finally:
            driver.quit()
            return comments_by_date

# 테스트 코드
if __name__ == "__main__":
    updater = UpdateDailyTossComments()
    symbol = "MSOX"
    result = updater.get_toss_comments(symbol)