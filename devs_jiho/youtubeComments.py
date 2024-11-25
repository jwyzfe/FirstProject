from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pandas as pd

def scroll_down(driver):
    """페이지를 아래로 스크롤하는 함수"""
    last_height = driver.execute_script("return document.documentElement.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
        time.sleep(2)
        new_height = driver.execute_script("return document.documentElement.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

def get_youtube_comments(channel_list):
    """유튜브 채널들의 동영상 댓글을 수집하는 함수"""
    driver = webdriver.Chrome()
    comments_data = []

    for channel in channel_list:
        # 채널의 동영상 목록 페이지 접속
        driver.get(f"https://www.youtube.com/@{channel}/videos")
        time.sleep(3)
        
        # 동영상 목록 스크롤
        scroll_down(driver)
        
        # 동영상 링크 수집
        video_elements = driver.find_elements(By.CSS_SELECTOR, "#contents ytd-rich-item-renderer")
        video_links = []
        for video in video_elements[:]:  # 최근 5개 동영상만 수집
            link = video.find_element(By.CSS_SELECTOR, "a#thumbnail").get_attribute("href")
            video_links.append(link)
            
        # 각 동영상 페이지 방문하여 댓글 수집
        for video_link in video_links:
            driver.get(video_link)
            time.sleep(3)
            
            # 동영상 제목 가져오기
            title = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#title h1 yt-formatted-string"))
            ).text
            
            # 댓글 섹션까지 스크롤
            driver.execute_script("window.scrollTo(0, 800)")
            time.sleep(3)
            
            # 댓글 로딩을 위해 스크롤
            scroll_down(driver)
            
            # 댓글 수집
            comments = driver.find_elements(By.CSS_SELECTOR, "#content-text")
            authors = driver.find_elements(By.CSS_SELECTOR, "#author-text")
            
            for author, comment in zip(authors, comments):
                comments_data.append({
                    'channel': channel,
                    'video_title': title,
                    'author': author.text.strip(),
                    'comment': comment.text.strip()
                })
    
    driver.quit()
    
    # 데이터프레임으로 변환하여 CSV 파일로 저장
    df = pd.DataFrame(comments_data)
    df.to_csv('youtube_comments_final.csv', index=False, encoding='utf-8-sig')
    return df

# 채널 리스트 설정
channel_list = ['hk_koreamarket']  # 원하는 채널 ID 추가

# 실행
comments_df = get_youtube_comments(channel_list)
print("댓글 수집이 완료되었습니다.")
