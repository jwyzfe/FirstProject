import requests
import pandas as pd
from bs4 import BeautifulSoup
import yfinance as yf

import os
import pandas as pd
import requests
from bs4 import BeautifulSoup

# 현재 작업 디렉토리 확인
print("현재 작업 디렉토리:", os.getcwd())

# 위키백과 페이지에서 S&P 500 종목 리스트 가져오기
url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies#S&P_500_component_stocks"
response = requests.get(url)

# 페이지의 HTML을 파싱
soup = BeautifulSoup(response.content, 'html.parser')

# 'wikitable' 클래스를 가진 테이블 찾기
table = soup.find('table', {'class': 'wikitable'})

# pandas로 테이블을 DataFrame으로 변환
df = pd.read_html(str(table))[0]

# 'Symbol'과 'Security' 열만 추출
df_sp500 = df[['Symbol', 'Security']]

# DataFrame을 CSV로 저장
df_sp500.to_csv('sp500_tickers_clean.csv', index=False)

print("S&P 500 Symbol과 Security 리스트가 'sp500_tickers_clean.csv'로 저장되었습니다.")
