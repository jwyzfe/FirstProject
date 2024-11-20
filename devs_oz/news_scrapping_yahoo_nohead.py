import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import requests
import pandas as pd
from pymongo import MongoClient

class NewsArticle:
    def __init__(self, title, news_url, contents):
        self.title = title
        self.news_url = news_url
        self.contents = contents

    def to_dict(self):
        return {
            'title': self.title,
            'news_url': self.news_url,
            'contents': self.contents,
        }

def scrape_news():
    # MongoDB 연결 설정
    client = MongoClient('mongodb://192.168.0.48:27017/')
    db = client['DB_SGMN']  # DB 이름
    collection = db['COL_SCRAPPING_YAHOO_WORK']  # 워크 컬렉션 이름

    # ChromeDriver 설정
    options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    # Yahoo Finance 페이지 열기
    driver.get('https://finance.yahoo.com/topic/stock-market-news/')

    # 페이지 로딩 완료 대기
    time.sleep(5)
    
    # 페이지를 조금 내리기 (스크롤)
    driver.execute_script("window.scrollBy(0, 10);")  # 300px 만큼 내리기
    time.sleep(2)
    
    # 기사 데이터 저장 리스트
    headline_news = []  # 일반 기사 저장
    h2_news = []  # h2 섹션 기사 저장
    scroll_count = 0  # 스크롤 횟수

    # 1단계: 일반 기사 스크랩 (밑에서 위 방향으로, 스크롤 최대 10번)
    try:    
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # 기사 스크랩
        headlines = list(reversed(soup.select('#nimbus-app > section > section > section > article  h3')))
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

                    # 뉴스 항목 생성
                    news_item = NewsArticle(title, link, content_text)
                    headline_news.append(news_item.to_dict())

                    # MongoDB에 데이터 삽입
                    collection.insert_one(news_item.to_dict())

                    # 새 기사 출력
                    print(f"Title: {news_item.title}")
                    print(f"URL: {news_item.news_url}")
                    print(f"Content: {news_item.contents[:200]}")  # Content의 앞 200자만 출력
                    print(f"--" * 10)

                except Exception as e:
                    print(f"Headline 기사 스크랩 중 오류 발생: {e} | Title: {title} | Link: {link}")
                    return None  # 예외 발생 시 함수 종료 및 None 반환
    except Exception as e:
        print(f"Headline 스크랩 중 오류 발생: {e}")
        

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

                    # 뉴스 항목 생성
                    news_item = NewsArticle(title, link, content_text)
                    h2_news.append(news_item.to_dict())

                    # MongoDB에 데이터 삽입
                    collection.insert_one(news_item.to_dict())

                    # 새 h2 기사 출력
                    print(f"Title: {news_item.title}")
                    print(f"URL: {news_item.news_url}")
                    print(f"Content: {news_item.contents[:200]}")  # Content의 앞 200자만 출력
                    print(f"--" * 10)

                except Exception as e:
                    print(f"h2 기사 스크랩 중 오류 발생: {e} | Title: {title} | Link: {link}")
                    return None  # 예외 발생 시 함수 종료 및 None 반환

        time.sleep(5)  # h2 섹션 업데이트 대기

    driver.quit()
    return "스크래핑 완료"

if __name__ == "__main__":
    result = scrape_news()
    print(result)