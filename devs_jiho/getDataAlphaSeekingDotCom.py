from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# 모든 라인에 breakpoint 설정
import pdb
import time

# 더 배워야 할것들
# 웹 요소의 텍스트 노드(#text) 웹 요소의 텍스트 노드(#text)를 찾으시려는 것 같네요. CSS 선택자를 수정하여 해결할 수 있습니다.

def main():
    # Chrome 옵션 설정
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    # 아래 옵션들 추가
    chrome_options.add_experimental_option("detach", True)  # 브라우저 자동 종료 방지
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])  # 자동화 표시 제거
    chrome_options.add_experimental_option('useAutomationExtension', False)  # 자동화 확장 기능 비활성화
    
    # 브라우저 인스턴스 생성
    browser = webdriver.Chrome(
        service=ChromeService(ChromeDriverManager().install()),
        options=chrome_options
    )
    
    try:
        # 로그인
        url = "https://seekingalpha.com/"
        browser.get(url)
        
        # 페이지 로딩 대기 추가
        wait = WebDriverWait(browser, 10)
        login_button = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".hidden.text-medium-2-r.md\:flex")))
        login_button.click()
        
        # 이메일 주소를 한 글자씩 입력
        email = "w01205250623@nate.com"
        for char in email:
            browser.find_element(By.CSS_SELECTOR, "label > div > input").send_keys(char)
            time.sleep(2)
        
        time.sleep(3)
        browser.find_element(By.CSS_SELECTOR, "#signInPasswordField").send_keys("gudijoayo!!")
        time.sleep(3)
        browser.find_element(By.CSS_SELECTOR, "form > button").click()
        
        # 로그인 후 대기
        time.sleep(3)
        
        # 여기서 stock_codes 리스트로 순회하면서 뉴스 수집
        stock_codes = ["MSFT", "AAPL", "GOOGL", "AMZN", "FB", "TSLA"]
        all_news = {}
        
        for code in stock_codes:
            print(f"{code} 뉴스 수집 중...")
            # browser 인스턴스를 get_news_from_seekingalpha 함수에 전달
            news = get_news_from_seekingalpha(code, browser)
            all_news[code] = news
            
    except Exception as e:
        print(f"에러 발생: {str(e)}")
    
    finally:
        # 모든 작업이 완료된 후에만 브라우저 종료
        browser.quit()

def get_news_from_seekingalpha(stock_code, browser):
    try:
        # 기존 browser 인스턴스 사용
        url = f"https://seekingalpha.com/symbol/{stock_code}/news"
        browser.get(url)
        
        # 나머지 뉴스 수집 로직
        wait = WebDriverWait(browser, 10)
        news_elements = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div:nth-child(3) > div > div.col-start-1.col-end-3 > div > section > div > div > div > div:nth-child(3) > article > div.min-w-0.grow > div > div > h3")))
        
        news_data = []
        for news in news_elements:
            # 뉴스 데이터 수집 로직
            # ...
        
            return news_data
        
    except Exception as e:
        print(f"{stock_code} 뉴스 크롤링 중 오류 발생: {str(e)}")
        return []

# 스크립트가 직접 실행될 때만 main() 함수 실행
if __name__ == "__main__":
    main()