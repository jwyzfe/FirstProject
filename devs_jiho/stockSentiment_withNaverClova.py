from selenium import webdriver
from selenium.webdriver.chrome.service import Service as CS
from webdriver_manager.chrome import ChromeDriverManager as CDM
from selenium.webdriver.chrome.options import Options # 정확히 뭐인지
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys # 내가 입력(input, 마치 마우스나 키보드 처럼) 할 것
from pymongo import MongoClient
import time
import json
import requests


client = MongoClient('mongodb://192.168.0.63:27017/')
webdriver_manager_directory = CDM().install()
browser = webdriver.Chrome(service=CS(webdriver_manager_directory))
browser.get("https://www.youtube.com/watch?v=qdbo9_KT_-w")
html = browser.page_source
print(html)
time.sleep(5)

youtubePageBody = browser.find_element(by=By.CSS_SELECTOR, value='body')

#commentBody = browser.find_elements(by=By.CSS_SELECTOR, value='#main')


def scrappingYoutubeComments(): #스크래핑 1번째
    results = []
    for downSevTimes in range(0,50):
        youtubePageBody.send_keys(Keys.PAGE_DOWN)
        time.sleep(1)
        commentBody = browser.find_elements(by=By.CSS_SELECTOR, value='#main') # 안쪽에다가 써야하는 이유?
        #로드된 댓글을 찾는 부분이 없었음
        for index, dataBundle in enumerate(commentBody[:2]):
            try: # 이 부분을 생각을 못했음
                textTag = f'#content-text > span'
                time.sleep(5)
                text = dataBundle.find_element(By.CSS_SELECTOR, textTag)
                itsDicResult = {"Comment_Text" : text.text}
                results.append(itsDicResult)
            except Exception as e:
                print("댓글 추출 오류:", e)
                continue
            return results

def sentimentClova(firstthing): #해석 2번째
    uri = f'https://naveropenapi.apigw.ntruss.com/sentiment-analysis/v1/analyze'
    itsHeaders = {'X-NCP-APIGW-API-KEY-ID':'0o3lns30e9','X-NCP-APIGW-API-KEY':
                  'an8HwGaZTa3OA0cgHQ9kWUbP4eT3RvPVk1z3mN0T','Content-Type':'application/json'}
    itsBodies = {"content" : firstthing[0]["Comment_Text"]} # 도큐멘트에 뭐라고 적혀있고 무엇을 넣어야지 되는지 자세히 알아볼것 
    response = requests.post(url=uri, headers=itsHeaders, data=json.dumps(itsBodies))
    itsContents = json.loads(response.text)
    return

def insertDB(secondthing): #저장 3번째
    db = client['joesDB']
    collection = db['movieBurningYoutubeComentsByLeedongjin']
    dbResult = collection.insert_one()
    print('Inserted user id:', dbResult.inserted_id) # inserted_id는 무엇을 의미하는지?
    


if __name__ == '__main__':
    firstthing = scrappingYoutubeComments()
    sentimentClova(firstthing)
    secondthing = sentimentClova(firstthing)
    pass
