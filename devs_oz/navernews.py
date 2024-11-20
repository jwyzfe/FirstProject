import yfinance as yf

# 티커 코드 (Apple Inc. - AAPL)
ticker = "AAPL"

# 주식 데이터 가져오기
stock_data = yf.Ticker(ticker)

# 최신 뉴스 가져오기
news = stock_data.news

# 뉴스 출력
for article in news:
    print(f"Title: {article['title']}")
    print(f"Link: {article['link']}")
    print(f"Published: {article['publisher']}")
    print("-" * 80)
    
    
import requests
from bs4 import BeautifulSoup

def get_article_content(url):
    # 기사 URL을 통해 HTML을 가져옵니다.
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # 기사 내용 추출 (여기서는 <div> 태그로 예시를 듭니다. 사이트 구조에 맞게 수정해야 합니다.)
    article_content = soup.find('div', {'class': '#nimbus-app > section > section > section > article > div > div.article-wrap.no-bb > div.body-wrap.yf-i23rhs'})  # 기사 본문이 있는 div 태그 (예시)
    
    if article_content:
        return article_content.get_text(strip=True)  # 기사 내용 텍스트 반환
    else:
        return None

# 예시 URL (실제 URL을 넣어야 합니다)
article_url = link  # 실제 기사 URL로 변경

article_content = get_article_content(article_url)

if article_content:
    print("기사 내용:")
    print(article_content)
else:
    print("기사 내용을 찾을 수 없습니다.")
