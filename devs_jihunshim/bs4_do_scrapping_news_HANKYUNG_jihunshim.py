
import requests
from bs4 import BeautifulSoup
import time

class bs4_scrapping:

    def do_scrapping(url):
        response = requests.get(url)
        soup = BeautifulSoup(response.text,'html.parser')
        news_list = soup.select('div.section-news-wrap > ul > li') # 되도록 짧은게 좋아 => 추후 프론트엔드 개발자에 의해 변경가능성 있어서

        results = [] 
        for news in news_list:
            title_link = news.select_one('li > div.txt-cont > h3 > a') # tbody > tr 스크래핑 
            date = news.select_one('div.txt-cont > span')
            
            news_content_url = title_link.attrs["href"]
            news_response = requests.get(news_content_url)
            time.sleep(1)
            news_soup = BeautifulSoup(news_response.text,'html.parser')
            content = news_soup.select_one('div#articletxt.article-body') 

            if content :
                # 결과를 딕셔너리 로 저장
                news_data = {
                    'title': title_link.text,
                    'link': news_content_url,
                    'date': date.text,
                    'content': content.text
                }
                results.append(news_data)   
            else :
                print(f"NO content {news_content_url}")
        return results
    
if __name__ == '__main__':
    page_list=['economy', 'financial-market', 'industry', 'politics', 'society', 'international']
    for url_page in page_list :
        for page_num in range(1,500):
            url = f'https://www.hankyung.com/{url_page}?page={page_num}'     
            bs4_scrapping.do_scrapping(url)
    pass