import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import requests

def scrape_news():
    # ChromeDriver 설정
    options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    # Yahoo Finance 페이지 열기
    driver.get('https://finance.yahoo.com/topic/stock-market-news/')

    # 기사 데이터 저장 리스트
    news_data = []
    count = 0  # 기사 개수 세기
    scroll_attempts = 0  # 스크롤 시도 횟수 제한
    MAX_SCROLL_ATTEMPTS = 5  # 최대 스크롤 횟수

    # 1단계: 기본 기사 스크랩
    while scroll_attempts < MAX_SCROLL_ATTEMPTS:
        time.sleep(2)  # 페이지 로딩 대기

        # 페이지 HTML 가져오기
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # 첫 번째 선택자로 기사 제목 스크랩
        headlines = soup.select('#nimbus-app > section > section > section > article > section h3')
        for headline in headlines:
            title = headline.text.strip()
            link = headline.find_parent('a').get('href', '')
            if link.startswith('/'):
                link = 'https://finance.yahoo.com' + link

            if title not in [news['title'] for news in news_data]:  # 중복 기사 제거
                # 뉴스 기사 페이지 요청하여 본문 가져오기
                response_content = requests.get(link)
                soup_content = BeautifulSoup(response_content.text, 'html.parser')
                content = soup_content.select_one('div.body-wrap.yf-i23rhs')

                # 본문이 있을 경우 저장
                if content:
                    news_item = {
                        'title': title,
                        'url': link,
                        'content': content.text.strip()  # 본문
                    }
                else:
                    news_item = {
                        'title': title,
                        'url': link,
                        'content': "본문을 찾을 수 없습니다."  # 본문이 없는 경우
                    }
                
                news_data.append(news_item)
                count += 1
                print(f" title : {title}")
                print(f" link :{link}")
                print(f"content: {content}")

        # 스크롤 시도
        last_height = driver.execute_script("return document.body.scrollHeight")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)  # 스크롤 후 로딩 대기
        new_height = driver.execute_script("return document.body.scrollHeight")

        # 더 이상 스크롤할 수 없는 경우 종료
        if new_height == last_height:
            print("더 이상 스크롤할 수 없습니다.")
            break

        scroll_attempts += 1

    # 2단계: Hero 섹션에서 업데이트된 기사 지속적으로 스크랩
    print("\nHero 섹션에서 업데이트된 기사:")
    while True:
        time.sleep(2)  # 페이지 로딩 대기

        # 페이지 HTML 가져오기
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # 두 번째 선택자로 Hero 섹션에서 기사 제목 스크랩
        hero_headlines = soup.select('#nimbus-app > section > section > section > article > section.topic-hero.yf-rxsm2g h')
        for hero_headline in hero_headlines:
            title = hero_headline.text.strip()
            link = hero_headline.find_parent('a').get('href', '')
            if link.startswith('/'):
                link = 'https://finance.yahoo.com' + link

            # 뉴스 기사 페이지 요청하여 본문 가져오기
            response_content = requests.get(link)
            soup_content = BeautifulSoup(response_content.text, 'html.parser')
            content = soup_content.select_one('div.body-wrap.yf-i23rhs')

            # 본문이 있을 경우 저장
            if content:
                news_item = {
                    'title': title,
                    'url': link,
                    'content': content.text.strip()  # 본문
                }
            else:
                news_item = {
                    'title': title,
                    'url': link,
                    'content': "본문을 찾을 수 없습니다."  # 본문이 없는 경우
                }

            if title not in [news['title'] for news in news_data]:  # 중복 기사 제거
                news_data.append(news_item)
                count += 1
                print(f" title : {title}")
                print(f" link :{link}")
                print(f"content: {content}")

    driver.quit()  # 브라우저 종료

    # 결과 출력
    print(f"\n총 {len(news_data)}개의 기사를 스크랩했습니다.")
    for news in news_data:
        print(f"Title: {news['title']}")
        print(f"URL: {news['url']}")
        print(f"Content: {news['content']}")  # 본문 출력
        print(f'--' * 10)

if __name__ == "__main__":
    scrape_news()
