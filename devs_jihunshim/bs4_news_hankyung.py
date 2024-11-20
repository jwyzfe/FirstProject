
import requests
from bs4 import BeautifulSoup
import time

class bs4_scrapping:

    def bs4_news_hankyung(url_list):
        all_results = []  # 모든 URL의 결과를 저장할 리스트
        
        for url in url_list:
            try:
                response = requests.get(url)
                soup = BeautifulSoup(response.text, 'html.parser')
                news_list = soup.select('div.section-news-wrap > ul > li')

                url_results = []  # 현재 URL의 결과를 저장할 리스트
                
                for news in news_list:
                    try:
                        title_link = news.select_one('li > div.txt-cont > h3 > a')
                        date = news.select_one('div.txt-cont > span')
                        
                        if title_link and date:  # title_link와 date가 존재하는 경우에만 처리
                            news_content_url = title_link.attrs["href"]
                            news_response = requests.get(news_content_url)
                            # time.sleep(1)  # 과도한 요청 방지
                            
                            news_soup = BeautifulSoup(news_response.text, 'html.parser')
                            content = news_soup.select_one('div#articletxt.article-body')

                            if content:
                                news_data = {
                                    'title': title_link.text.strip(),
                                    'link': news_content_url,
                                    'date': date.text.strip(),
                                    'content': content.text.strip(),
                                    'source_url': url  # 원본 URL 추가
                                }
                                url_results.append(news_data)
                            else:
                                # print(f"No content found for: {news_content_url}")
                                continue
                        
                    except Exception as e:
                        print(f"Error processing news item: {str(e)}")
                        continue
                
                # 현재 URL의 결과를 전체 결과 리스트에 추가
                all_results.extend(url_results)
                # print(f"Processed URL: {url}, Found {len(url_results)} articles")
                
            except Exception as e:
                print(f"Error processing URL {url}: {str(e)}")
                continue
        
        # 결과 요약 출력
        # print(f"\nTotal articles collected: {len(all_results)}")
        
        return all_results
    
if __name__ == '__main__':
    page_list=['economy', 'financial-market', 'industry', 'politics', 'society', 'international']
    for url_page in page_list :
        for page_num in range(1,2):
            url = f'https://www.hankyung.com/{url_page}?page={page_num}'     
            bs4_scrapping.bs4_news_hankyung(url)
    pass