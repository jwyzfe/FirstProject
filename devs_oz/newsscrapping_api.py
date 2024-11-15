from newsapi import NewsApiClient

# NewsAPI 클라이언트 초기화 (자신의 API Key를 넣으세요)
newsapi = NewsApiClient(api_key='796ab1eb1d924086adf753a53c92b734')

# Finance 카테고리에서 주식 관련 뉴스 가져오기
def get_finance_news(page=1):
    try:
        # 'category'는 'business'로 설정하여 금융 관련 뉴스 가져오기
        top_headlines = newsapi.get_top_headlines(
            category='business',        # Finance 카테고리 관련 뉴스
            language='en',              # 영어 기사
            sort_by='relevancy',        # 관련성 높은 순으로 정렬
            page=page                   # 페이지 번호 (여러 페이지가 있을 경우)
        )
        
        # 기사 내용 출력 (메타데이터)
        if top_headlines['status'] == 'ok':
            articles = top_headlines['articles']
            for article in articles:
                print(f"Title: {article['title']}")
                print(f"Description: {article['description']}")
                print(f"URL: {article['url']}")
                print(f"Published At: {article['publishedAt']}")
                print('-' * 80)
        else:
            print("Failed to fetch news.")
    
    except Exception as e:
        print(f"Error: {e}")

# 예시: Finance 카테고리의 모든 뉴스 가져오기
get_finance_news(page=1)


# from newsapi import NewsApiClient

# # NewsAPI 클라이언트 초기화 (자신의 API Key를 넣으세요)
# newsapi = NewsApiClient(api_key='796ab1eb1d924086adf753a53c92b734')

# # /v2/top-headlines/sources로 지원되는 소스 확인
# sources = newsapi.get_sources()

# # 소스 출력
# for source in sources['sources']:
#     print(f"Source Name: {source['name']}, ID: {source['id']}")
