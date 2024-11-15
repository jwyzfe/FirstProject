import requests
from bs4 import BeautifulSoup

def main():
    respone = requests.get(f'https://finance.yahoo.com/topic/stock-market-news/')
    soup = BeautifulSoup(respone.text, 'html.parser')
    titles_link = soup.select('#Fin-Stream > ul > li h3 > a')

    for title_link in titles_link:
        news_content_url = title_link.attrs['href']
        
        # Full URL로 연결 (상대 경로일 경우)
        if news_content_url.startswith('/'):
            news_content_url = 'https://finance.yahoo.com' + news_content_url
        
        respone_content = requests.get(news_content_url)
        soup_content = BeautifulSoup(respone_content.text, 'html.parser')

        
        content = soup_content.select_one('#nimbus-app > section > section > section > article > div > div.article-wrap.no-bb > div.body-wrap.yf-i23rhs')
        
        if content:  # 본문이 있을 경우에만 제목과 본문 출력
            print(f'title : {title_link.text.strip()}')
            print(f'news_content_url : {news_content_url}')
            print(f'content : {content.text.strip()}')
            print(f'--' * 10)
        else:
            print(f' ')
            print(f'--' * 10)

if __name__ == '__main__':
    main()
