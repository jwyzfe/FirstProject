from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import json

def get_company_codes():
    driver = webdriver.Chrome()
    driver.get("https://dart.fss.or.kr/dsae001/main.do#none")
    company_data = {}  # 회사명과 코드를 저장할 딕셔너리
    
    try:
        while True:
            # 현재 페이지의 모든 회사 정보 수집 완료 플래그
            page_completed = False
            processed_count = 0  # 처리된 회사 수 추적
            
            while not page_completed:
                try:
                    # 현재 페이지의 모든 회사 목록 가져오기
                    companies = WebDriverWait(driver, 10).until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "#corpTabel > tbody > tr > td.tL.ellipsis > span > a"))
                    )
                    
                    # 아직 처리하지 않은 회사만 클릭
                    if processed_count < len(companies):
                        companies[processed_count].click()
                        time.sleep(1)
                        
                        # 회사명 가져오기
                        company_name = WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "#corpDetailTabel > tbody > tr:nth-child(3) > td"))
                        ).text
                        
                        # 회사 코드 가져오기
                        code_element = WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "#corpDetailTabel > tbody > tr:nth-child(7) > td"))
                        )
                        company_data[company_name] = code_element.text
                        processed_count += 1
                    else:
                        page_completed = True
                        
                except:
                    page_completed = True
            
            # 다음 페이지로 이동
            current_page_text = driver.find_element(By.CSS_SELECTOR, ".pageSkip .on").text
            next_page_found = False
            
            for i in range(3, 9):
                try:
                    next_button = driver.find_element(By.CSS_SELECTOR, 
                        f"#listContents > div.psWrap > div.pageSkip > ul > li:nth-child({i}) > a")
                    if int(next_button.text) > int(current_page_text):
                        next_button.click()
                        next_page_found = True
                        time.sleep(2)
                        break
                except:
                    continue
            
            if not next_page_found:
                break
                
    finally:
        driver.quit()
    
    return company_data

if __name__ == "__main__":
    company_data = get_company_codes()
    print(f"총 {len(company_data)}개의 회사 정보를 수집했습니다.")
    
    # JSON 파일로 저장
    with open('company_codes.json', 'w', encoding='utf-8') as f:
        json.dump({
            "total_count": len(company_data),
            "companies": company_data
        }, f, ensure_ascii=False, indent=4)
    
    print("회사 정보가 company_codes.json 파일로 저장되었습니다.")
