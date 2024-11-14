from bs4 import BeautifulSoup
import os
import re

def find_financial_values(soup):
    # 재무 관련 키워드 정의
    financial_keywords = [
        'net income',
        'net profit',
        'net earnings',
        'net revenue',
        'total revenue',
        'operating income',
        'gross profit'
    ]
    
    print("\n=== 주요 재무 데이터 ===")
    
    # 모든 테이블 검색
    tables = soup.find_all('table')
    for table in tables:
        rows = table.find_all('tr')
        for row in rows:
            # 텍스트 데이터 추출
            cols = row.find_all(['td', 'th'])
            row_text = ' '.join([col.get_text().strip().lower() for col in cols])
            
            # 키워드 매칭
            for keyword in financial_keywords:
                if keyword in row_text:
                    # 숫자 데이터 찾기 (통화 기호와 숫자)
                    values = [col.get_text().strip() for col in cols if col.get_text().strip()]
                    if values:
                        print(f"\n{values[0]}:")
                        for value in values[1:]:
                            if re.search(r'[\d,]+', value):  # 숫자가 포함된 값만 출력
                                print(f"  {value}")

def parse_10q_file(file_path):
    try:
        # HTML 파일 읽기
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
            
        # BeautifulSoup으로 파싱
        soup = BeautifulSoup(content, 'html.parser')
        
        # 재무 데이터 찾기
        find_financial_values(soup)
        
        # 모든 테이블 찾기
        tables = soup.find_all('table')
        print(f"\n총 {len(tables)}개의 테이블을 찾았습니다.\n")
        
        for i, table in enumerate(tables, 1):
            # 테이블 제목 찾기
            title_div = table.find_previous('div')
            if title_div:
                title = title_div.get_text().strip()
                if title:
                    print(f"\n테이블 {i} 제목: {title}")
            
            # 테이블 데이터 추출
            rows = table.find_all('tr')
            for row in rows:
                cols = row.find_all(['td', 'th'])
                row_data = [col.get_text().strip() for col in cols if col.get_text().strip()]
                if row_data:  # 빈 행 제외
                    print(row_data)
            
            print("\n" + "="*50)

    except Exception as e:
        print(f"파싱 중 오류 발생: {str(e)}")

if __name__ == "__main__":
    # 파일 경로 설정
    file_path = "mvst-20240930.htm"
    
    if os.path.exists(file_path):
        parse_10q_file(file_path)
    else:
        print(f"파일을 찾을 수 없습니다: {file_path}")