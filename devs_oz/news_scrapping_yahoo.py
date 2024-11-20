import time
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

def yahoo_scrapping():
    # ChromeDriver 설정
    # options = webdriver.ChromeOptions()
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # GUI 없이 실행
    chrome_options.add_argument("--no-sandbox")  # 샌드박스 비활성화
    chrome_options.add_argument("--disable-dev-shm-usage")  # /dev/shm 사용 비활성화
    
    # ChromeDriver 경로 설정
    service = ChromeService('/usr/bin/chromedriver')  # ChromeDriver 경로

    # ChromeDriver 실행
    driver = webdriver.Chrome(service=service, options=chrome_options)
    # driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    # Yahoo Finance 페이지 열기
    driver.get('https://finance.yahoo.com/topic/stock-market-news/')
    
    # 스크롤을 내리면서 기사를 30개 정도 가져오기
    news_data = []  # 뉴스 데이터를 담을 리스트 초기화
    count = 0  # 기사의 개수 세기
    while count < 10:
        # 페이지가 로드될 때까지 기다리기
        time.sleep(2)
        
        # 현재 페이지의 HTML 가져오기
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # 뉴스 제목 링크 찾기
        titles_link = soup.select('#Fin-Stream > ul > li h3 > a')

        # 뉴스 제목과 URL 가져오기
        for title_link in titles_link:
            news_content_url = title_link.attrs['href']
            
            # 상대 경로일 경우 절대 URL로 연결
            if news_content_url.startswith('/'):
                news_content_url = 'https://finance.yahoo.com' + news_content_url
            
            # 뉴스 기사 페이지 요청
            respone_content = requests.get(news_content_url)
            soup_content = BeautifulSoup(respone_content.text, 'html.parser')

            # 본문 추출
            content = soup_content.select_one('div.body-wrap.yf-i23rhs')

            # 본문이 있을 경우에만 저장
            if content:
                news_item = {
                    "title": title_link.text.strip(),  # 제목
                    "url": news_content_url,           # 뉴스 URL
                    "content": content.text.strip()    # 본문
                }
            else:
                news_item = {
                    "title": title_link.text.strip(),  # 제목
                    "url": news_content_url,           # 뉴스 URL
                    "content": "본문을 찾을 수 없습니다."  # 본문이 없으면 메시지 저장
                }
            
            news_data.append(news_item)  # 리스트에 딕셔너리 추가
            count += 1  # 기사의 개수 증가

            if count >= 10:
                break
        
        # 스크롤 내리기 (페이지 맨 아래로)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        
        # 로딩 시간을 기다리기
        time.sleep(2)
    
    # 결과 출력 (예시)
    print(f"총 {len(news_data)}개의 기사가 저장되었습니다.")
    for item in news_data:
        print(f"Title: {item['title']}")
        print(f"URL: {item['url']}")
        print(f"Content: {item['content']}")  # 본문 일부만 출력 (200자)
        print(f'--' * 10)

    driver.quit()  # 브라우저 종료

if __name__ == '__main__':
    yahoo_scrapping()  # 함수 호출
