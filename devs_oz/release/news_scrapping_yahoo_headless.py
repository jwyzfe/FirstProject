import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import requests
import pandas as pd

from selenium.webdriver.chrome.options import Options


# mongo DB 동작
from pymongo import MongoClient
import platform

# 직접 만든 class나 func을 참조하려면 꼭 필요 => main processor가 경로를 잘 몰라서 알려주어야함.
import sys
import os
# 현재 파일의 두 단계 상위 디렉토리(FirstProject)를 path에 추가
current_dir = os.path.dirname(os.path.abspath(__file__))  # /manage
parent_dir = os.path.dirname(current_dir)  # /schedulers
project_dir = os.path.dirname(parent_dir)  # /FirstProject
sys.path.append(project_dir)
from commons.mongo_insert_recode import connect_mongo as connect_mongo_insert
from commons.config_reader import read_config # config read 용       
    
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


class yahoo_finance_scrap:

    def get_browser():
        options = Options()
        
        # 운영체제 확인
        current_os = platform.system().lower()
        
        if current_os == 'linux':
            # Linux 환경 설정
            options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")

            # 추가 옵션들
            options.add_argument("--window-size=1920,1080")  # 충분한 뷰포트 크기
            options.add_argument("--disable-gpu")
            options.add_argument("--enable-javascript")
            browser = webdriver.Chrome(service=Service('/usr/bin/chromedriver'), options=options)
        else:
            # Windows 또는 다른 환경 설정
            webdriver_manager_directory = ChromeDriverManager().install()
            browser = webdriver.Chrome(service=Service(webdriver_manager_directory), options=options)
        
        return browser

    def scrape_news_infloop_version():

        options = Options()
        options.add_argument("--headless")  # GUI 없이 실행
        options.add_argument("--no-sandbox")  # 샌드박스 비활성화
        options.add_argument("--disable-dev-shm-usage")  # /dev/shm 사용 비활성화

        driver = webdriver.Chrome(service=Service('/usr/bin/chromedriver'), options=options)

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

                        # # 새 기사 출력
                        # print(f"Title: {news_item.title}")
                        # print(f"URL: {news_item.news_url}")
                        # print(f"Date: {news_item.date}")
                        # print(f"Content: {news_item.contents[:200]}")  # Content의 앞 200자만 출력
                        # print(f"--" * 10)

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

                        # # 새 h2 기사 출력
                        # print(f"Title: {news_item.title}")
                        # print(f"URL: {news_item.news_url}")
                        # print(f"Date: {news_item.date}")
                        # print(f"Content: {news_item.contents[:200]}")  # Content의 앞 200자만 출력
                        # print(f"--" * 10)

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

                # 최종 데이터 출력 (또는 저장 작업)
                # print(combined_news_df)


                # 반복적으로 스크래핑을 수행하려면 시간 지연 후 계속 실행
                # break
                time.sleep(10)
            except Exception as e:
                time.sleep(10)
            finally:
                driver.quit()
                if not combined_news_df.empty:
                    return combined_news_df
                else:
                    return pd.DataFrame()
        # return combined_news_df

    def scrape_news_schedule_version():
        driver = yahoo_finance_scrap.get_browser()
        combined_news_df = pd.DataFrame()  # 초기 빈 DataFrame 생성

        try:
            # Yahoo Finance 페이지 열기
            driver.get('https://finance.yahoo.com/topic/stock-market-news/')
            time.sleep(5)

            # 기사 데이터 저장 리스트
            headline_news = []
            h2_news = []

            # 페이지 소스 가져오기
            soup = BeautifulSoup(driver.page_source, 'html.parser')

            # 1단계: 일반 기사 스크랩
            headlines = list(reversed(soup.select('#nimbus-app > section > section > section > article h3')))
            for headline in headlines:
                try:
                    title = headline.text.strip()
                    parent_a = headline.find_parent('a')
                    if parent_a is None:
                        continue
                        
                    link = parent_a.get('href', '')
                    if not link:
                        continue

                    if link.startswith('/'):
                        link = 'https://finance.yahoo.com' + link

                    # 기사 본문 스크랩
                    response_content = requests.get(link)
                    soup_content = BeautifulSoup(response_content.text, 'html.parser')
                    date_tag = soup_content.select_one('#nimbus-app time')
                    date = date_tag.text.strip() if date_tag else 'Unknown Date'
                    content = soup_content.select_one('div.body-wrap.yf-i23rhs')
                    content_text = content.text.strip() if content else 'No Content'

                    news_item = NewsArticle(title, link, date, content_text)
                    headline_news.append(news_item.to_dict())

                except Exception as e:
                    print(f"Headline 기사 스크랩 중 오류 발생: {e}")
                    continue

            # 2단계: h2 섹션 기사 스크랩
            h2_headlines = soup.select('#nimbus-app article > section h2')
            for h2_headline in h2_headlines:
                try:
                    title = h2_headline.text.strip()
                    parent_a = h2_headline.find_parent('a')
                    if parent_a is None:
                        continue
                        
                    link = parent_a.get('href', '')
                    if not link:
                        continue

                    if link.startswith('/'):
                        link = 'https://finance.yahoo.com' + link

                    # 기사 본문 스크랩
                    response_content = requests.get(link)
                    soup_content = BeautifulSoup(response_content.text, 'html.parser')
                    date_tag = soup_content.select_one('#nimbus-app time')
                    date = date_tag.text.strip() if date_tag else 'Unknown Date'
                    content = soup_content.select_one('div.body-wrap.yf-i23rhs')
                    content_text = content.text.strip() if content else 'No Content'

                    news_item = NewsArticle(title, link, date, content_text)
                    h2_news.append(news_item.to_dict())

                except Exception as e:
                    print(f"h2 기사 스크랩 중 오류 발생: {e}")
                    continue

            # DataFrame 생성 및 병합
            if headline_news or h2_news:  # 데이터가 있는 경우에만 DataFrame 생성
                headline_news_df = pd.DataFrame(headline_news) if headline_news else pd.DataFrame()
                h2_news_df = pd.DataFrame(h2_news) if h2_news else pd.DataFrame()
                
                if not headline_news_df.empty or not h2_news_df.empty:
                    combined_news_df = pd.concat([headline_news_df, h2_news_df], ignore_index=True)
                    combined_news_df.drop_duplicates(subset='news_url', keep='first', inplace=True)

        except Exception as e:
            print(f"Error news scrap: {e}")
        finally:
            driver.quit()
            return combined_news_df  # 빈 DataFrame이어도 반환


if __name__ == "__main__":

    config = read_config()
    ip_add = config['MongoDB_local']['ip_add']
    db_name = config['MongoDB_local']['db_name']
    col_name = f'COL_NEWS_YAHOO_DAILY' # 데이터 읽을 collection
    # MongoDB 서버에 연결 
    client = MongoClient(ip_add) 

    combined_news_df = yahoo_finance_scrap.scrape_news_schedule_version()
    # print(combined_news_df)
    result_list = connect_mongo_insert.insert_recode_in_mongo(client, db_name, col_name, combined_news_df)



