from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import time

'''
# ChromeDriver 설정
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

# URL 열기
driver.get("https://www.reddit.com/t/finance/")

# 'post-title-t3'가 포함된 ID를 가진 태그 모두 추출
elements = driver.find_elements("css selector", "[id*=post-title-t3]")  

# 추출한 태그 출력
for element in elements:
    print(f" Text: {element.text.strip()}")

# 브라우저 종료
driver.quit()
'''


# ChromeDriver 설정
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

# 대상 웹사이트로 이동
driver.get("https://www.reddit.com/t/finance/")  # 대상 웹사이트 URL

# 페이지 로딩 대기
time.sleep(5)

try:
    # Title 출력
    '''
    title_element = driver.find_element(By.CSS_SELECTOR, "#post-title-t3")
    title = title_element.text.strip()
    print(f"Title: {title}")
    print(f"----------------------------------------------")
    '''
    # User 출력
    user_element = driver.find_element(By.CSS_SELECTOR, " faceplate-hovercard > a > span")
    user = user_element.text.strip()
    print(f"User: {user}")
    print(f"----------------------------------------------")
    # Date 출력
    date_element = driver.find_element(By.CSS_SELECTOR, " faceplate-timeago > time")
    date = date_element.get_attribute("datetime")
    print(f"Date: {date}")
    print(f"----------------------------------------------")
    # JavaScript로 클릭
    driver.execute_script("arguments[0].click();", title_element)
    time.sleep(5)  # 페이지 로딩 대기

    # Content 출력
    content_element = driver.find_element(By.CSS_SELECTOR, "#t3_1fkc4es-post-rtjson-content")
    content = content_element.text.strip()
    print(f"Content: {content}")
    print(f"----------------------------------------------")
    # Comment User 출력
    comment_user_element = driver.find_element(By.CSS_SELECTOR, "#comment-tree > shreddit-comment:nth-child(3) > div.ml-xs.py-\\[2px\\].min-w-0 > div > div > div > div > faceplate-tracker > a")
    comment_user = comment_user_element.text.strip()
    print(f"Comment User: {comment_user}")
    print(f"----------------------------------------------")
    # Comment Date 출력
    comment_date_element = driver.find_element(By.CSS_SELECTOR, "#comment-tree > shreddit-comment:nth-child(3) > div.ml-xs.py-\\[2px\\].min-w-0 > div > div > div > a > faceplate-timeago > time")
    comment_date = comment_date_element.get_attribute("datetime")
    print(f"Comment Date: {comment_date}")
    print(f"----------------------------------------------")
    comment_content_element = driver.find_element(By.CSS_SELECTOR, "#-post-rtjson-content")
    comment_content = comment_content_element.text.strip()
    print(f"Comment Content: {comment_content}")
except Exception as e:
    print(f"Error occurred: {e}")
