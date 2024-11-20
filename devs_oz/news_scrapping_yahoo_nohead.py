import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import requests
import pandas as pd

class NewsArticle:
    def __init__(self, title, url, content):
        self.title = title
        self.url = url
        self.content = content

    def to_dict(self):
        return {
            'title': self.title,
            'url': self.url,
            'content': self.content,
        }

def scrape_news():
    # ChromeDriver 설정
    options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    # Yahoo Finance 페이지 열기
    driver.get('https://finance.yahoo.com/topic/stock-market-news/')

    # 기사 데이터 저장 리스트
    headline_news = []  # 일반 기사 저장
    h2_news = []  # h2 섹션 기사 저장
    scroll_count = 0  # 스크롤 횟수

    # 초기 데이터프레임 설정
    combined_df = pd.DataFrame(columns=['title', 'url', 'content'])  # 빈 데이터프레임 생성

    print("기사 스크랩 시작 (밑에서 위 방향으로)...")

    # 스크롤 최하단으로 이동
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)

    # 1단계: 일반 기사 스크랩 (밑에서 위 방향으로, 스크롤 최대 10번)
    while scroll_count < 10:
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # 기사 스크랩
        headlines = soup.select('#nimbus-app > section > section > section > article  h3')
        for headline in headlines:
            title = headline.text.strip()
            link = headline.find_parent('a').get('href', '')
            if link.startswith('/'):
                link = 'https://finance.yahoo.com' + link

            if title not in [news['title'] for news in headline_news]:
                try:
                    # 기사 본문 스크랩
                    response_content = requests.get(link)
                    soup_content = BeautifulSoup(response_content.text, 'html.parser')
                    content = soup_content.select_one('div.body-wrap.yf-i23rhs')
                    content_text = content.text.strip() if content else ''

                    news_item = NewsArticle(title, link, content_text)
                    headline_news.append(news_item.to_dict())

                    # 실시간 데이터프레임 갱신
                    headline_df = pd.DataFrame([news_item.to_dict()])
                    combined_df = pd.concat([combined_df, headline_df], ignore_index=True)

                    # 새 기사 출력
                    print(f"Title: {news_item.title}")
                    print(f"URL: {news_item.url}")
                    print(f"Content: {news_item.content}...")
                    print(f"--" * 10)

                except Exception as e:
                    print(f"Headline 기사 스크랩 중 오류 발생: {e} | Title: {title} | Link: {link}")

        # 위로 스크롤
        driver.execute_script("window.scrollBy(0, -window.innerHeight);")
        scroll_count += 1
        time.sleep(2)

    # 2단계: h2 섹션 기사 스크랩
    while True:
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # h2 섹션 기사 스크랩
        h2_headlines = soup.select('#nimbus-app article > section h2')
        for h2_headline in h2_headlines:
            title = h2_headline.text.strip()
            link = h2_headline.find_parent('a').get('href', '')
            if link.startswith('/'):
                link = 'https://finance.yahoo.com' + link

            if title not in [news['title'] for news in h2_news]:
                try:
                    # 기사 본문 스크랩
                    response_content = requests.get(link)
                    soup_content = BeautifulSoup(response_content.text, 'html.parser')
                    content = soup_content.select_one('div.body-wrap.yf-i23rhs')
                    content_text = content.text.strip() if content else ''

                    news_item = NewsArticle(title, link, content_text)
                    h2_news.append(news_item.to_dict())

                    # 실시간 데이터프레임 갱신
                    h2_df = pd.DataFrame([news_item.to_dict()])
                    combined_df = pd.concat([combined_df, h2_df], ignore_index=True)

                    # 새 h2 기사 출력
                    print(f"Title: {news_item.title}")
                    print(f"URL: {news_item.url}")
                    print(f"Content: {news_item.content}...")
                    print(f"--" * 10)

                except Exception as e:
                    print(f"h2 기사 스크랩 중 오류 발생: {e} | Title: {title} | Link: {link}")

        time.sleep(5)  # h2 섹션 업데이트 대기

    

if __name__ == "__main__":
    scrape_news()
