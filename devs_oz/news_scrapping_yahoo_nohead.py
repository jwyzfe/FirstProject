import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import requests
import pandas as pd
from pymongo import MongoClient

class NewsArticle:
    def __init__(self, title, news_url, date, contents):
        self.title = title
        self.news_url = news_url
        self.date = date
        self.contents = contents

    def to_dict(self):
        return {
            'title': self.title,
            'news_url': self.news_url,
            'date': self.date,
            'contents': self.contents,
        }

def scrape_news():
    
    client = MongoClient('mongodb://192.168.0.48:27017/')
    db = client['DB_SGMN']  # DB 이름
    collection = db['COL_SCRAPPING_YAHOO_WORK']
    # ChromeDriver 설정
    options = webdriver.ChromeOptions()
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    # Yahoo Finance 페이지 열기
    driver.get('https://finance.yahoo.com/topic/stock-market-news/')

    # 페이지 로딩 완료 대기
    time.sleep(5)

    # 기사 데이터 저장 리스트
    headline_news = []  # 일반 기사 저장
    h2_news = []  # h2 섹션 기사 저장

    while True:
        try:
            # 페이지 소스 가져오기
            soup = BeautifulSoup(driver.page_source, 'html.parser')

            # 1단계: 일반 기사 스크랩
            headlines = list(reversed(soup.select('#nimbus-app > section > section > section > article h3')))
            for headline in headlines:
                title = headline.text.strip()
                link = headline.find_parent('a').get('href', '')
                if link.startswith('/'):
                    link = 'https://finance.yahoo.com' + link

                try:
                    # 기사 본문 스크랩
                    response_content = requests.get(link)
                    soup_content = BeautifulSoup(response_content.text, 'html.parser')
                    date_tag = soup_content.select_one('#nimbus-app time')  # 날짜 태그
                    date = date_tag.text.strip() if date_tag else 'Unknown Date'
                    content = soup_content.select_one('div.body-wrap.yf-i23rhs')  # 본문 태그
                    content_text = content.text.strip() if content else 'No Content'

                    # 뉴스 항목 생성
                    news_item = NewsArticle(title, link, date, content_text)
                    headline_news.append(news_item.to_dict())

                    # 새 기사 출력
                    print(f"Title: {news_item.title}")
                    print(f"URL: {news_item.news_url}")
                    print(f"Date: {news_item.date}")
                    print(f"Content: {news_item.contents[:200]}")  # Content의 앞 200자만 출력
                    print(f"--" * 10)

                except Exception as e:
                    print(f"Headline 기사 스크랩 중 오류 발생: {e} | Title: {title} | Link: {link}")
                    continue

            # 2단계: h2 섹션 기사 스크랩
            h2_headlines = soup.select('#nimbus-app article > section h2')
            for h2_headline in h2_headlines:
                title = h2_headline.text.strip()
                link = h2_headline.find_parent('a').get('href', '')
                if link.startswith('/'):
                    link = 'https://finance.yahoo.com' + link

                try:
                    # 기사 본문 스크랩
                    response_content = requests.get(link)
                    soup_content = BeautifulSoup(response_content.text, 'html.parser')
                    date_tag = soup_content.select_one('#nimbus-app time')  # 날짜 태그
                    date = date_tag.text.strip() if date_tag else 'Unknown Date'
                    content = soup_content.select_one('div.body-wrap.yf-i23rhs')  # 본문 태그
                    content_text = content.text.strip() if content else 'No Content'

                    # 뉴스 항목 생성
                    news_item = NewsArticle(title, link, date, content_text)
                    h2_news.append(news_item.to_dict())

                    # 새 h2 기사 출력
                    print(f"Title: {news_item.title}")
                    print(f"URL: {news_item.news_url}")
                    print(f"Date: {news_item.date}")
                    print(f"Content: {news_item.contents[:200]}")  # Content의 앞 200자만 출력
                    print(f"--" * 10)

                except Exception as e:
                    print(f"h2 기사 스크랩 중 오류 발생: {e} | Title: {title} | Link: {link}")
                    continue

            # 중복된 URL 제거 (drop_duplicates 사용)
            headline_news_df = pd.DataFrame(headline_news)
            h2_news_df = pd.DataFrame(h2_news)

            # 두 DataFrame을 합치기
            combined_news_df = pd.concat([headline_news_df, h2_news_df], ignore_index=True)

            # 중복된 뉴스 URL 제거
            combined_news_df.drop_duplicates(subset='news_url', keep='first', inplace=True)
            
            # MongoDB에 저장
            records = combined_news_df.to_dict(orient='records')  # DataFrame을 MongoDB 형식으로 변환
            if records:
                collection.insert_many(records)

            # 최종 데이터 출력 (또는 저장 작업)
            print(combined_news_df)


            # 반복적으로 스크래핑을 수행하려면 시간 지연 후 계속 실행
            time.sleep(10)
        except Exception as e:
            time.sleep(10)

    driver.quit()
    return " 스크래핑 완료"

if __name__ == "__main__":
    combined_news_df = scrape_news()
    print(combined_news_df)
