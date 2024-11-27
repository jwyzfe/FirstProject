
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.options import Options

# 직접 만든 class나 func을 참조하려면 꼭 필요 => main processor가 경로를 잘 몰라서 알려주어야함.
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                
from commons.config_reader import read_config # config read 용     

import pandas as pd
import re
import time
import platform
from selenium.webdriver.common.keys import Keys # 내가 입력(input, 마치 마우스나 키보드 처럼) 할 것
from selenium.webdriver import ActionChains as ac

class comment_scrap_stocktwits : 
    
    @staticmethod
    def get_all_links_from_element(element, visited_elements=None, max_depth=5, current_depth=0):
        """
        요소의 모든 하위 요소를 재귀적으로 탐색하여 링크를 수집하는 함수
        
        Args:
            element: 웹 요소
            visited_elements: 이미 방문한 요소들의 집합
            max_depth: 최대 재귀 깊이
            current_depth: 현재 재귀 깊이
        """
        # 방문한 요소 추적을 위한 집합 초기화
        if visited_elements is None:
            visited_elements = set()
        
        # 깊이 제한 확인
        if current_depth >= max_depth:
            return []
        
        # 이미 방문한 요소인지 확인
        element_id = id(element)
        if element_id in visited_elements:
            return []
        
        # 현재 요소를 방문 목록에 추가
        visited_elements.add(element_id)
        
        links = []
        
        try:
            # href 속성 확인
            href = element.get_attribute('href')
            if href and (href.startswith('http') or href.startswith('//')):
                links.append(href)
            
            # src 속성 확인
            src = element.get_attribute('src')
            if src and (src.startswith('http') or src.startswith('//')):
                links.append(src)
            
            # 모든 하위 요소 탐색
            children = element.find_elements(By.XPATH, './/*')
            for child in children:
                child_links = comment_scrap_stocktwits.get_all_links_from_element(
                    child,
                    visited_elements,
                    max_depth,
                    current_depth + 1
                )
                links.extend(child_links)
        except Exception as e:
            print(f"Error processing element: {e}")
            pass
        
        return list(set(links))  # 중복 제거


    def get_data(browser: webdriver.Chrome, symbol) -> pd.DataFrame:

        data = {
            "symbol": [],
            "datetime": [],
            "content": [],
            "links": []  # 링크를 저장할 새로운 컬럼
        }
        
        # 게시글 리스트 가져오기
        list_tags = browser.find_elements(by=By.CSS_SELECTOR, value='.SymbolPage_feedContainer__9RhsB article')
        
        for article in list_tags:
            try:
                # 컨텐츠와 시간 요소 찾기
                content_element = article.find_element(By.CSS_SELECTOR, '.StreamMessage_messageContent__T_T27')
                time_element = article.find_element(By.CSS_SELECTOR, '.StreamMessage_heading__GU_fe time')
                
                # 데이터 추출
                content = content_element.text
                datetime_str = time_element.get_attribute('datetime')
                
                # 모든 링크 수집
                all_links = comment_scrap_stocktwits.get_all_links_from_element(content_element)
                
                # 데이터 저장
                data["symbol"].append(symbol)
                data["datetime"].append(datetime_str)
                data["content"].append(content)
                data["links"].append(all_links)  # 수집된 모든 링크 저장
                
            except Exception as e:
                print(f"Error processing article: {e}")
                continue
        
        # DataFrame 생성 및 반환
        return pd.DataFrame(data)
    
    def scroll_down_and_login(browser: webdriver.Chrome, symbol) -> webdriver.Chrome:
                
        target_url = f'https://stocktwits.com/symbol/{symbol}'
        # - 주소 입력(https://www.w3schools.com/)
        browser.get(target_url)
        
        '''
        AAPL
        한 칸
        #Layout > div.Main_mainContainer-CORE1-1151__HSY7M.Main_main__3C253.min-h-screen.w-full > div.Body_container__f_XJ0.flex.flex-column.desktopXxl\|pr-0.tabletSm-down\|pb-12.Body_containerBorder__3T8GJ.border-primary-hairline.border-l.tabletSm-down\|border-x-0 > div > div > div.SymbolPage_symbolPageBorder__gg8Gr.border-primary-hairline.border-r > div.SymbolPage_feedContainer__9RhsB.tabletSm\|bg-primary-background.tabletMd\|bg-transparent > div > div > div:nth-child(2) > div > div > div:nth-child(2) > div:nth-child(2) > div.infinite-scroll-component__outerdiv > div > div:nth-child(4) > article > div > div.StreamMessage_maxStreamContentColumn__dlgyj.overflow-hidden.pl-1.max-w-full
        #Layout > div.Main_mainContainer-CORE1-1151__HSY7M.Main_main__3C253.min-h-screen.w-full > div.Body_container__f_XJ0.flex.flex-column.desktopXxl\|pr-0.tabletSm-down\|pb-12.Body_containerBorder__3T8GJ.border-primary-hairline.border-l.tabletSm-down\|border-x-0 > div > div > div.SymbolPage_symbolPageBorder__gg8Gr.border-primary-hairline.border-r > div.SymbolPage_feedContainer__9RhsB.tabletSm\|bg-primary-background.tabletMd\|bg-transparent > div > div > div:nth-child(2) > div > div > div:nth-child(2) > div:nth-child(2) > div.infinite-scroll-component__outerdiv > div > div:nth-child(4) > article > div > div:nth-child(2)
        아이디 + 시간
        #Layout > div.Main_mainContainer-CORE1-1151__HSY7M.Main_main__3C253.min-h-screen.w-full > div.Body_container__f_XJ0.flex.flex-column.desktopXxl\|pr-0.tabletSm-down\|pb-12.Body_containerBorder__3T8GJ.border-primary-hairline.border-l.tabletSm-down\|border-x-0 > div > div > div.SymbolPage_symbolPageBorder__gg8Gr.border-primary-hairline.border-r > div.SymbolPage_feedContainer__9RhsB.tabletSm\|bg-primary-background.tabletMd\|bg-transparent > div > div > div:nth-child(2) > div > div > div:nth-child(2) > div:nth-child(2) > div.infinite-scroll-component__outerdiv > div > div:nth-child(4) > article > div > div.StreamMessage_maxStreamContentColumn__dlgyj.overflow-hidden.pl-1.max-w-full > div.StreamMessage_heading__GU_fe.flex.items-center.justify-between > div
        시간
        #Layout > div.Main_mainContainer-CORE1-1151__HSY7M.Main_main__3C253.min-h-screen.w-full > div.Body_container__f_XJ0.flex.flex-column.desktopXxl\|pr-0.tabletSm-down\|pb-12.Body_containerBorder__3T8GJ.border-primary-hairline.border-l.tabletSm-down\|border-x-0 > div > div > div.SymbolPage_symbolPageBorder__gg8Gr.border-primary-hairline.border-r > div.SymbolPage_feedContainer__9RhsB.tabletSm\|bg-primary-background.tabletMd\|bg-transparent > div > div > div:nth-child(2) > div > div > div:nth-child(2) > div:nth-child(2) > div.infinite-scroll-component__outerdiv > div > div:nth-child(4) > article > div > div.StreamMessage_maxStreamContentColumn__dlgyj.overflow-hidden.pl-1.max-w-full > div.StreamMessage_heading__GU_fe.flex.items-center.justify-between > div > div > a > time
        #Layout > div.Main_mainContainer-CORE1-1151__HSY7M.Main_main__3C253.min-h-screen.w-full > div.Body_container__f_XJ0.flex.flex-column.desktopXxl\|pr-0.tabletSm-down\|pb-12.Body_containerBorder__3T8GJ.border-primary-hairline.border-l.tabletSm-down\|border-x-0 > div > div > div.SymbolPage_symbolPageBorder__gg8Gr.border-primary-hairline.border-r > div.SymbolPage_feedContainer__9RhsB.tabletSm\|bg-primary-background.tabletMd\|bg-transparent > div > div > div:nth-child(2) > div > div > div:nth-child(2) > div:nth-child(2) > div.infinite-scroll-component__outerdiv > div > div:nth-child(3) > article > div > div:nth-child(2) > div.StreamMessage_heading__GU_fe.flex.items-center.justify-between > div > div > a > time
        #Layout > div.Main_mainContainer-CORE1-1151__HSY7M.Main_main__3C253.min-h-screen.w-full > div.Body_container__f_XJ0.flex.flex-column.desktopXxl\|pr-0.tabletSm-down\|pb-12.Body_containerBorder__3T8GJ.border-primary-hairline.border-l.tabletSm-down\|border-x-0 > div > div > div.SymbolPage_symbolPageBorder__gg8Gr.border-primary-hairline.border-r > div.SymbolPage_feedContainer__9RhsB.tabletSm\|bg-primary-background.tabletMd\|bg-transparent > div > div > div:nth-child(2) > div > div > div:nth-child(2) > div:nth-child(2) > div.infinite-scroll-component__outerdiv > div > div:nth-child(1) > article > div > div.StreamMessage_maxStreamContentColumn__dlgyj.overflow-hidden.pl-1.max-w-full > div.StreamMessage_heading__GU_fe.flex.items-center.justify-between > div > div > a > time
        줄 글
        #Layout > div.Main_mainContainer-CORE1-1151__HSY7M.Main_main__3C253.min-h-screen.w-full > div.Body_container__f_XJ0.flex.flex-column.desktopXxl\|pr-0.tabletSm-down\|pb-12.Body_containerBorder__3T8GJ.border-primary-hairline.border-l.tabletSm-down\|border-x-0 > div > div > div.SymbolPage_symbolPageBorder__gg8Gr.border-primary-hairline.border-r > div.SymbolPage_feedContainer__9RhsB.tabletSm\|bg-primary-background.tabletMd\|bg-transparent > div > div > div:nth-child(2) > div > div > div:nth-child(2) > div:nth-child(2) > div.infinite-scroll-component__outerdiv > div > div:nth-child(4) > article > div > div.StreamMessage_maxStreamContentColumn__dlgyj.overflow-hidden.pl-1.max-w-full > div.StreamMessage_messageContentDefault__cHL1P.StreamMessage_messageContent__T_T27.pr-4.StreamMessage_mediaContainer__f56_R.tabletMd\|overflow-hidden > div > div:nth-child(1) > div
        #Layout > div.Main_mainContainer-CORE1-1151__HSY7M.Main_main__3C253.min-h-screen.w-full > div.Body_container__f_XJ0.flex.flex-column.desktopXxl\|pr-0.tabletSm-down\|pb-12.Body_containerBorder__3T8GJ.border-primary-hairline.border-l.tabletSm-down\|border-x-0 > div > div > div.SymbolPage_symbolPageBorder__gg8Gr.border-primary-hairline.border-r > div.SymbolPage_feedContainer__9RhsB.tabletSm\|bg-primary-background.tabletMd\|bg-transparent > div > div > div:nth-child(2) > div > div > div:nth-child(2) > div:nth-child(3) > div.infinite-scroll-component__outerdiv > div > div:nth-child(6) > article > div > div:nth-child(2) > div.StreamMessage_messageContentDefault__cHL1P.StreamMessage_messageContent__T_T27.pr-4
        #Layout > div.Main_mainContainer-CORE1-1151__HSY7M.Main_main__3C253.min-h-screen.w-full > div.Body_container__f_XJ0.flex.flex-column.desktopXxl\|pr-0.tabletSm-down\|pb-12.Body_containerBorder__3T8GJ.border-primary-hairline.border-l.tabletSm-down\|border-x-0 > div > div > div.SymbolPage_symbolPageBorder__gg8Gr.border-primary-hairline.border-r > div.SymbolPage_feedContainer__9RhsB.tabletSm\|bg-primary-background.tabletMd\|bg-transparent > div > div > div:nth-child(2) > div > div > div:nth-child(2) > div:nth-child(2) > div.infinite-scroll-component__outerdiv > div > div:nth-child(3) > article > div > div:nth-child(2) > div.StreamMessage_messageContentDefault__cHL1P.StreamMessage_messageContent__T_T27.pr-4
        #Layout > div.Main_mainContainer-CORE1-1151__HSY7M.Main_main__3C253.min-h-screen.w-full > div.Body_container__f_XJ0.flex.flex-column.desktopXxl\|pr-0.tabletSm-down\|pb-12.Body_containerBorder__3T8GJ.border-primary-hairline.border-l.tabletSm-down\|border-x-0 > div > div > div.SymbolPage_symbolPageBorder__gg8Gr.border-primary-hairline.border-r > div.SymbolPage_feedContainer__9RhsB.tabletSm\|bg-primary-background.tabletMd\|bg-transparent > div > div > div:nth-child(2) > div > div > div:nth-child(2) > div:nth-child(2) > div.infinite-scroll-component__outerdiv > div > div:nth-child(4) > article > div > div.StreamMessage_maxStreamContentColumn__dlgyj.overflow-hidden.pl-1.max-w-full > div.StreamMessage_messageContentDefault__cHL1P.StreamMessage_messageContent__T_T27.pr-4.StreamMessage_mediaContainer__f56_R.tabletMd\|overflow-hidden
        #Layout > div.Main_mainContainer-CORE1-1151__HSY7M.Main_main__3C253.min-h-screen.w-full > div.Body_container__f_XJ0.flex.flex-column.desktopXxl\|pr-0.tabletSm-down\|pb-12.Body_containerBorder__3T8GJ.border-primary-hairline.border-l.tabletSm-down\|border-x-0 > div > div > div.SymbolPage_symbolPageBorder__gg8Gr.border-primary-hairline.border-r > div.SymbolPage_feedContainer__9RhsB.tabletSm\|bg-primary-background.tabletMd\|bg-transparent > div > div > div:nth-child(2) > div > div > div:nth-child(2) > div:nth-child(2) > div.infinite-scroll-component__outerdiv > div > div:nth-child(9) > article > div > div.StreamMessage_maxStreamContentColumn__dlgyj.overflow-hidden.pl-1.max-w-full > div.StreamMessage_messageContentDefault__cHL1P.StreamMessage_messageContent__T_T27.pr-4.StreamMessage_mediaContainer__f56_R.tabletMd\|overflow-hidden
        
        로그인 화면 email 칸
        #Layout > div.OnboardingContainer_container__lsIXK.relative.w-full.tabletSm\|h-screen.tabletSm\|w-full.tabletSm\|grid.tabletSm\|grid-flow-row.tabletSm-down\|grid-cols-none > div.flex.flex-col.justify-center.items-center.bg-white > div > div.Onboarding_form__Didqp.mb-5.tabletSm-down\|mb-10 > form > div.TextInput_textField__aVKWB.flex.flex-col.relative.w-full.pt-2.mb-4.TextInput_default__IqFgl.TextInput_error__4GOBG > input
        #Layout > div.OnboardingContainer_container__lsIXK.relative.w-full.tabletSm\\|h-screen.tabletSm\\|w-full.tabletSm\\|grid.tabletSm\\|grid-flow-row.tabletSm-down\\|grid-cols-none > div.flex.flex-col.justify-center.items-center.bg-white > div > div.Onboarding_form__Didqp.mb-5.tabletSm-down\\|mb-10 > form > div:nth-child(1) > input
        
        로그인 화면 text 나오게 하는 부분 
        #Layout > div.OnboardingContainer_container__lsIXK.relative.w-full.tabletSm\|h-screen.tabletSm\|w-full.tabletSm\|grid.tabletSm\|grid-flow-row.tabletSm-down\|grid-cols-none > div.flex.flex-col.justify-center.items-center.bg-white > div > div.Onboarding_form__Didqp.mb-5.tabletSm-down\|mb-10 > form > div:nth-child(1) > label
        로그인 화면 password 칸
        #Layout > div.OnboardingContainer_container__lsIXK.relative.w-full.tabletSm\|h-screen.tabletSm\|w-full.tabletSm\|grid.tabletSm\|grid-flow-row.tabletSm-down\|grid-cols-none > div.flex.flex-col.justify-center.items-center.bg-white > div > div.Onboarding_form__Didqp.mb-5.tabletSm-down\|mb-10 > form > div:nth-child(2) > input
        로그인 화면 버튼
        #Layout > div.OnboardingContainer_container__lsIXK.relative.w-full.tabletSm\|h-screen.tabletSm\|w-full.tabletSm\|grid.tabletSm\|grid-flow-row.tabletSm-down\|grid-cols-none > div.flex.flex-col.justify-center.items-center.bg-white > div > div.Onboarding_form__Didqp.mb-5.tabletSm-down\|mb-10 > form > button
        로그인 화면 existing 링크 
        body > div.ReactModalPortal > div > div > div > div.flex.flex-col.justify-between.items-center.SignUpButtons_socialMediaButtons__UVFBA.w-full.gap-y-2.max-w-\[268px\].h-\[162px\].w-full > a.SignUpButtons_logInLink__vrXFQ.text-blue-ada.text-sm.h-\[18px\]
        '''
        # # test ver
        # for num in range(0,5):
        #     ac(browser).key_down(Keys.PAGE_DOWN).perform() # 브라우저를 선택해야해 # 탭별로 컨트롤 가능하기 때문
        #     # browser.implicitly_wait(50) # 로딩이 끝난걸 알아서 탐지 # sleep 보다 동작 느림 # handleless 방식도 있음
        #     time.sleep(1) 
        #     current_height = browser.execute_script(f'return document.documentElement.scrollTop')
        config = read_config()
        id = config['stocktwits']['id']
        pw = config['stocktwits']['pw']

        # 로그인 페이지 나오게 하는 부분 로그인 되어 있으면 그냥 이거만 
        for num in range(0,25):
            ac(browser).key_down(Keys.PAGE_DOWN).perform() # 브라우저를 선택해야해 # 탭별로 컨트롤 가능하기 때문
            # browser.implicitly_wait(50) # 로딩이 끝난걸 알아서 탐지 # sleep 보다 동작 느림 # handleless 방식도 있음
            time.sleep(1) 
            current_height = browser.execute_script(f'return document.documentElement.scrollTop')

        # 로그인 페이지 나온 이후에 해야할 동작
        # 최초 google auth 외에 직접 로그인 하는 창으로 이동
        # 로그인 상태 확인
        try:
            time.sleep(1)
            # 로그인 모달이 있는지 확인 (로그인되지 않은 상태)
            login_modal = browser.find_element(By.CSS_SELECTOR, '.ReactModalPortal')
            
            blue_link_tag = f'body > div.ReactModalPortal > div > div > div > div.flex.flex-col.justify-between.items-center.SignUpButtons_socialMediaButtons__UVFBA.w-full.gap-y-2.max-w-\\[268px\\].h-\\[162px\\].w-full > a.SignUpButtons_logInLink__vrXFQ.text-blue-ada.text-sm.h-\\[18px\\]'
            element_blue_link = browser.find_element(by=By.CSS_SELECTOR, value=blue_link_tag)
            element_blue_link.click()

            time.sleep(1)
            label_tag = f'#Layout > div.OnboardingContainer_container__lsIXK.relative.w-full.tabletSm\\|h-screen.tabletSm\\|w-full.tabletSm\\|grid.tabletSm\\|grid-flow-row.tabletSm-down\\|grid-cols-none > div.flex.flex-col.justify-center.items-center.bg-white > div > div.Onboarding_form__Didqp.mb-5.tabletSm-down\\|mb-10 > form > div:nth-child(1) > label'
            element_label = browser.find_element(by=By.CSS_SELECTOR, value=label_tag)
            element_label.click()

            time.sleep(1)
            id_tag = f'#Layout > div.OnboardingContainer_container__lsIXK.relative.w-full.tabletSm\\|h-screen.tabletSm\\|w-full.tabletSm\\|grid.tabletSm\\|grid-flow-row.tabletSm-down\\|grid-cols-none > div.flex.flex-col.justify-center.items-center.bg-white > div > div.Onboarding_form__Didqp.mb-5.tabletSm-down\\|mb-10 > form > div:nth-child(1) > input'
            id_str = id # readconfig 구현하기 headless 도
            element_id = browser.find_element(by=By.CSS_SELECTOR, value=id_tag)
            element_id.send_keys(id_str)

            time.sleep(1)
            pw_tag = f'#Layout > div.OnboardingContainer_container__lsIXK.relative.w-full.tabletSm\\|h-screen.tabletSm\\|w-full.tabletSm\\|grid.tabletSm\\|grid-flow-row.tabletSm-down\\|grid-cols-none > div.flex.flex-col.justify-center.items-center.bg-white > div > div.Onboarding_form__Didqp.mb-5.tabletSm-down\\|mb-10 > form > div:nth-child(2) > input'
            pw_str = pw # readconfig 구현하기 headless 도
            element_pw = browser.find_element(by=By.CSS_SELECTOR, value=pw_tag)
            element_pw.send_keys(pw_str)

            time.sleep(1)
            btn_tag = f'#Layout > div.OnboardingContainer_container__lsIXK.relative.w-full.tabletSm\\|h-screen.tabletSm\\|w-full.tabletSm\\|grid.tabletSm\\|grid-flow-row.tabletSm-down\\|grid-cols-none > div.flex.flex-col.justify-center.items-center.bg-white > div > div.Onboarding_form__Didqp.mb-5.tabletSm-down\\|mb-10 > form > button'
            element_btn = browser.find_element(by=By.CSS_SELECTOR, value=btn_tag)
            element_btn.click() 

            # 로그인 페이지 나오게 하는 부분
            for num in range(0,25):
                ac(browser).key_down(Keys.PAGE_DOWN).perform() # 브라우저를 선택해야해 # 탭별로 컨트롤 가능하기 때문
                # browser.implicitly_wait(50) # 로딩이 끝난걸 알아서 탐지 # sleep 보다 동작 느림 # handleless 방식도 있음
                time.sleep(1) 
                current_height = browser.execute_script(f'return document.documentElement.scrollTop')

        except Exception as e:
            # 로그인 모달이 없는 경우 (이미 로그인된 상태)
            print("Already logged in or login not required")
            pass

        return browser

    
    def get_browser():
        options = Options()
        
        # 운영체제 확인
        current_os = platform.system().lower()
        
        if current_os == 'linux':
            # Linux 환경 설정
            options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            browser = webdriver.Chrome(service=Service('/usr/bin/chromedriver'), options=options)
        else:
            # Windows 또는 다른 환경 설정
            webdriver_manager_directory = ChromeDriverManager().install()
            browser = webdriver.Chrome(service=Service(webdriver_manager_directory), options=options)
        
        return browser

    def run_stocktwits_scrap_list(symbol_list) -> pd.DataFrame:

        # # options = Options()
        # # options.add_argument("--headless")  # GUI 없이 실행
        # # options.add_argument("--no-sandbox")  # 샌드박스 비활성화
        # # options.add_argument("--disable-dev-shm-usage")  # /dev/shm 사용 비활성화
        # # browser = webdriver.Chrome(service=Service('/usr/bin/chromedriver'), options=options)
        
        # webdriver_manager_directory = ChromeDriverManager().install() # 딱 한번 수행이라 밖에
        # browser = webdriver.Chrome(service=Service(webdriver_manager_directory))
        browser = comment_scrap_stocktwits.get_browser()

        # 결과를 저장할 데이터프레임 리스트
        df_list = []
        
        for symbol in symbol_list:
            try:
                comment_scrap_stocktwits.scroll_down_and_login(browser, symbol)
                df = comment_scrap_stocktwits.get_data(browser, symbol)
                df_list.append(df)
            except Exception as e:
                print(f"Error processing symbol {symbol}: {e}")
                continue
        
        # 브라우저 종료
        browser.quit()
        
        # 모든 데이터프레임 합치기
        if df_list:
            return pd.concat(df_list, ignore_index=True)
        else:
            return pd.DataFrame()  # 빈 데이터프레임 반환
    

if __name__ == '__main__':

    # need symbol list version 
    symbol = 'AAPL'
    symbol = 'NVDA'
    symbol = 'MSFT'
    symbol = ['AAPL','NVDA','MSFT','GOOGL','CSCO']
    try:
        # 시작 시간 기록
        start_time = time.time()
    
        case_data = comment_scrap_stocktwits.run_stocktwits_scrap_list(symbol)

        # 실행 시간 계산
        elapsed_time = time.time() - start_time
        print(f"Execution time: {elapsed_time:.2f} seconds") 

        print(case_data)
    except Exception as e :
        print(e)
    # finally:
    #     browser.quit()
    