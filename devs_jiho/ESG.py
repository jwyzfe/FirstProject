# Selenium 관련 필요한 모듈들을 가져옵니다
from selenium import webdriver  # 웹 브라우저 자동화를 위한 기본 모듈
from selenium.webdriver.common.by import By  # 요소를 찾기 위한 방법을 제공하는 모듈
from selenium.webdriver.support.ui import WebDriverWait  # 특정 조건까지 대기하기 위한 모듈
from selenium.webdriver.support import expected_conditions as EC  # 대기 조건을 지정하기 위한 모듈
from selenium.common.exceptions import TimeoutException, NoSuchElementException  # 예외 처리를 위한 모듈
import json  # JSON 처리를 위한 모듈 추가

def crawl_esg_data():
    # ESG 데이터를 저장할 리스트를 초기화합니다
    esg = []
    
    # Chrome 웹드라이버를 초기화합니다
    driver = webdriver.Chrome()
    
    # ESG 페이지 URL을 지정하고 접속합니다
    url = "https://esg.krx.co.kr/contents/02/02020000/ESG02020000.jsp"
    driver.get(url)
    
    try:
        while True:  # 무한 루프를 시작합니다 (마지막 페이지까지 반복)
            # 테이블의 모든 행(tr)을 찾을 때까지 최대 10초 대기합니다
            rows = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((
                    By.CSS_SELECTOR, 
                    "#gridtableeccbc87e4b5ce2fe28308fd9f2a7baf3 > tbody > tr"
                ))
            )
            
            # 찾은 모든 행을 순회합니다
            for row in rows:
                row_data = row.text.split()  # 행의 텍스트를 공백으로 분리합니다
                esg.append(row_data)  # 데이터를 리스트에 추가합니다
                print(row_data)  # 확인을 위해 출력합니다
            
            try:
                # 다음 페이지 버튼을 찾습니다
                next_button = driver.find_element(
                    By.CSS_SELECTOR, 
                    "#pagenavia87ff679a2f3e71d9181a67b7542122c > ul > li.next > a"
                )
                
                # 다음 버튼이 비활성화되어 있으면 루프를 종료합니다
                if not next_button.is_enabled():
                    break
                    
                # 다음 페이지 버튼을 클릭합니다
                next_button.click()
                
                # 페이지가 변경될 때까지 대기합니다
                # 이전 페이지의 첫 번째 행이 사라질 때까지 기다립니다
                WebDriverWait(driver, 10).until(
                    EC.staleness_of(rows[0])
                )
                
            except NoSuchElementException:
                # 다음 페이지 버튼을 찾을 수 없는 경우 (마지막 페이지) 루프를 종료합니다
                break
                
            except TimeoutException:
                # 페이지 로딩이 10초 이상 걸리는 경우 루프를 종료합니다
                break
                
    finally:
        # 예외가 발생하더라도 반드시 웹드라이버를 종료합니다
        driver.quit()
        
    # 수집된 데이터를 원하는 형식으로 변환
    formatted_data = []
    for row in esg:
        # 디버깅을 위한 출력 추가
        print(f"Original row data: {row}")
        
        try:
            data_dict = {
                "회사명": row[0] if len(row) > 0 else "",
                "KCGS(종합등급)": row[1] if len(row) > 1 else "",
                "KCGS(환경)": row[2] if len(row) > 2 else "",
                "KCGS(사회)": row[3] if len(row) > 3 else "",
                "KCGS(지배구조)": row[4] if len(row) > 4 else "",
                "한국ESG연구소": row[5] if len(row) > 5 else "",
                "서스틴베스트": row[6] if len(row) > 6 else "",
                "Moody's": row[7] if len(row) > 7 else "",
                "MSCI": row[8] if len(row) > 8 else "",
                "S&P": row[9] if len(row) > 9 else "",
            }
            formatted_data.append(data_dict)
        except Exception as e:
            print(f"Error processing row: {row}")
            print(f"Error message: {str(e)}")
            continue
    
    # JSON 파일로 저장
    with open('esg_data.json', 'w', encoding='utf-8') as f:
        json.dump(formatted_data, f, ensure_ascii=False, indent=4)
    
    return formatted_data  # 형식이 변환된 데이터를 반환

# 이 파일이 직접 실행될 때만 크롤링을 시작합니다
if __name__ == "__main__":
    esg_data = crawl_esg_data()
    print(f"총 {len(esg_data)}개의 ESG 데이터가 수집되었습니다.")
    print("데이터가 'esg_data.json' 파일로 저장되었습니다.")
