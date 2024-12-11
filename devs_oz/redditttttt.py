import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# Selenium WebDriver 설정
options = webdriver.ChromeOptions()
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# 뽐뿌 주식 게시판 URL
url = "https://www.ppomppu.co.kr/zboard/zboard.php?id=stock"
driver.get(url)

# 페이지 로딩 대기
time.sleep(5)

# 스크래핑 데이터 저장 리스트
scraped_data = []

try:
    # BeautifulSoup로 HTML 파싱
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    # 게시글 제목 및 링크 추출
    rows = soup.select('table.list_table tbody tr')
    for row in rows:
        # 게시글 제목
        title_tag = row.select_one(' tr > td> a')
        if not title_tag:
            continue  # 제목이 없는 경우 스킵

        title = title_tag.text.strip()
        link = title_tag['href']
        if link.startswith('/'):
            link = f"https://www.ppomppu.co.kr{link}"

        # 게시글 날짜
        date_tag = row.select_one('#topTitle  li:nth-child(2)')
        date = date_tag.text.strip() if date_tag else 'Unknown Date'

        # 게시글 내용 추출
        driver.get(link)
        time.sleep(3)  # 페이지 로딩 대기
        detail_soup = BeautifulSoup(driver.page_source, 'html.parser')
        content_tag = detail_soup.select_one('tr tr table > tbody')
        content = content_tag.text.strip() if content_tag else 'No Content'

        # 데이터 추가
        news_item = {
            'title': title,
            'news_url': link,
            'date': date,
            'contents': content
        }
        scraped_data.append(news_item)

        # 데이터 출력
        print(f"Title: {title}")
        print(f"URL: {link}")
        print(f"Date: {date}")
        print(f"Content: {content[:200]}")  # 본문 앞 200자만 출력
        print("-" * 50)

except Exception as e:
    print(f"스크래핑 중 오류 발생: {e}")

finally:
    # 브라우저 닫기
    driver.quit()
