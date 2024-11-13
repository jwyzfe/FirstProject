
# 스크래퍼 import 
from apscheduler.schedulers.background import BackgroundScheduler
# mongo DB 동작
from pymongo import MongoClient
# time.sleep
import time
from datetime import datetime, timedelta  

# selenium driver 
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService

# 직접 만든 class 
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                
from commons.mongo_insert_recode import connect_mongo
from commons.api_send_requester import ApiRequester
from commons.templates.sel_iframe_courtauction import iframe_test
from commons.templates.bs4_do_scrapping import bs4_scrapping

# 직접 구현한 부분을 import 해서 scheduler에 등록
from devs_shlee.api_stockprice_yfinance import api_stockprice_yfinance

def api_test_func():
    # api
    city_list = ['도쿄','괌','모나코']
    key_list = ['lat', 'lon']
    pub_key = '39fb7b1c6d4e11e7483aabcb737ce7b0'
    for city in city_list:
        base_url = f'https://api.openweathermap.org/geo/1.0/direct'
        
        params={}
        params['q'] = city
        params['appid'] = pub_key

        result_geo = ApiRequester.send_api(base_url, params, key_list)

        base_url = f'https://api.openweathermap.org/data/2.5/forecast'
        
        params_w = {}
        for geo in result_geo:
            for key in key_list:
                params_w[key] = geo[key]
        params_w['appid'] = pub_key
        result_cont = ApiRequester.send_api(base_url, params_w)

        print(result_cont)

# common 에 넣을 예정
def register_job_with_mongo(client, ip_add, db_name, col_name, func, insert_data):

    try:
        result_data = func(*insert_data)
        if client is None:
            client = MongoClient(ip_add) # 관리 신경써야 함.
        result_list = connect_mongo.insert_recode_in_mongo(client, db_name, col_name, result_data)       # print(f'insert id list count : {len(result_list.inserted_ids)}')
    except Exception as e :
        print(e)
        client.close()

    return 

def chunk_list(lst, n):
    """리스트를 n 크기로 나누는 제너레이터 함수"""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
        
def run():

    # 스케쥴러 등록 
    # mongodb 가져올 수 있도록
    ip_add = f'mongodb://localhost:27017/'
    db_name = f'DB_SGMN' # db name 바꾸기
    col_name = f'collection_name' # collection name 바꾸기

    # MongoDB 서버에 연결 
    client = MongoClient(ip_add) 

    scheduler = BackgroundScheduler()
    
    url = f'http://underkg.co.kr/news'

    # 여기에 함수 등록 
    # 넘길 변수 없으면 # insert_data = []
    symbols = [
    "NVDA", "AAPL", "MSFT", "AMZN", "META", "GOOGL", "TSLA", "GOOG", "BRK.B", "AVGO",
    "JPM", "LLY", "UNH", "XOM", "V", "MA", "COST", "HD", "PG", "JNJ",
    "WMT", "NFLX", "CRM", "BAC", "ABBV", "ORCL", "CVX", "MRK", "WFC", "KO",
    "AMD", "CSCO", "PEP", "ACN", "ADBE", "LIN", "MCD", "NOW", "TMO", "ABT",
    "GE", "TXN", "IBM", "INTU", "PM", "CAT", "GS", "ISRG", "QCOM", "DIS",
    "AMGN", "CMCSA", "VZ", "BKNG", "MS", "RTX", "AXP", "SPGI", "T", "LOW",
    "DHR", "AMAT", "NEE", "PGR", "UBER", "PFE", "ETN", "UNP", "BLK", "HON",
    "TJX", "C", "BX", "BSX", "COP", "SYK", "VRTX", "PANW", "ADP", "FI",
    "PLTR", "BMY", "LMT", "MU", "TMUS", "SCHW", "VICI", "WEC", "AON", "MSI",
    "CDNS", "WM", "CME", "CMG", "GD", "ZTS", "MCK", "USB", "WELL", "CRWD",
    "TDG", "CTAS", "EOG", "CL", "MCO", "CEG", "ITW", "EMR", "NOC", "MMM",
    "COF", "ORLY", "TGT", "CSX", "CVS", "APD", "WMB", "BDX", "ADSK", "MAR",
    "HCA", "FDX", "GM", "FCX", "AJG", "CARR", "OKE", "ECL", "SLB", "TFC",
    "FTNT", "HLT", "NSC", "PCAR", "ROP", "ABNB", "TRV", "SRE", "URI", "BK",
    "JCI", "NXPI", "FICO", "DLR", "AFL", "RCL", "SPG", "AMP", "GWW", "AZO",
    "PSX", "PSA", "KMI", "MPC", "ALL", "AEP", "O", "LHX", "VST", "CPRT",
    "CMI", "DHI", "D", "PWR", "NEM", "AIG", "FIS", "FAST", "MET", "PAYX",
    "ROST", "TEL", "MSCI", "HWM", "DFS", "KVUE", "CCI", "PCG", "KMB", "PRU",
    "AME", "VLO", "AXON", "F", "BKR", "PEG", "ODFL", "COR", "RSG", "TRGP",
    "IR", "IT", "LEN", "CBRE", "DAL", "OTIS", "VRSK", "CTVA", "CTSH", "EW",
    "DELL", "KR", "MNST", "HES", "A", "GEHC", "VMC", "CHTR", "YUM", "EXC",
    "SYY", "EA", "XEL", "NUE", "GLW", "MLM", "ACGL", "IQV", "MCHP", "KDP",
    "STZ",
    "HPQ", "LULU", "RMD", "IDXX", "MTB", "GIS", "WAB", "EXR", "DD", "HIG",
    "IRM", "OXY", "ED", "HUM", "FANG", "EFX", "AVB", "NDAQ", "VICI", "GRMN",
    "DOW", "EIX", "ETR", "WTW", "CNC", "FITB", "MPWR", "ROK", "WEC", "CSGP",
    "EBAY", "TSCO", "XYL", "ANSS", "RJF", "ON", "CAH", "UAL", "GPN", "TTWO",
    "PPG", "STT", "KHC", "HPE", "KEYS", "NVR", "DXCM", "DOV", "PHM", "LDOS",
    "DECK", "VTR", "FTV", "HAL", "BRO", "MTD", "CDW", "BR", "CHD", "HSY",
    "TROW", "TYL", "EQT", "AWK", "SYF", "CPAY", "GDDY", "SW", "VLTO", "HBAN",
    "EQR", "NTAP", "BIIB", "ADM", "HUBB", "CCL", "DTE", "PPL", "AEE", "DVN",
    "RF", "WST", "SBAC", "CINF", "IFF", "PTC", "EXPE", "TDY", "WY", "ATO",
    "WAT", "ZBH", "LYB", "WDC", "STE", "ES", "K", "PKG", "NTRS", "STX",
    "BLDR", "FE", "STLD", "CBOE", "CFG", "WBD", "ZBRA", "FSLR", "COO", "CMS",
    "LH", "CLX", "MAS", "BBY", "PNR", "BAX", "IEX", "ARE", "GPC", "KIM",
    "EXPD", "TSN", "DG", "TXT", "AVY", "GEN", "NI", "MRO", "EG", "JBHT",
    "ALGN", "DOC", "DPZ", "VTRS", "VRSN", "JBL", "LNT", "CF", "L", "EL",
    "AMCR", "RVTY", "APTV", "EVRG", "MRNA", "POOL", "ROL", "NDSN", "FFIV",
    "SWKS", "SWK", "EPAM", "AKAM", "UDR", "CAG", "INCY", "CPT", "ALB", "JKHY",
    "CHRW", "JNPR", "HST", "DAY", "ALLE", "BG", "UHS", "DLTR", "NCLH", "SJM",
    "REG", "KMX", "EMN", "BXP", "TPR", "TECH", "GNRC", "SMCI", "LW", "CRL",
    "IPG", "PAYC", "NWSA", "AIZ", "CTLT", "TAP", "ERIE", "PNW", "LKQ", "MKTX",
    "FOXA", "SOLV", "AES", "GL", "TFX", "AOS", "MOS", "HRL", "CPB", "CZR",
    "HSIC", "FRT", "ENPH", "CE", "RL", "MGM", "HAS", "MTCH", "IVZ", "APA",
    "HII", "WYNN", "BWA", "MHK", "BF.B", "FMC", "DVA", "PARA", "WBA", "BEN",
    "QRVO", "FOX", "AMTM", "NWS"
    ]
    # insert_data = [symbols] # [val1,val2,val3]
    
    # func_list = [
    #     # {"func" : bs4_scrapping.do_scrapping, "args" : insert_data},
    #     {"func" : api_stockprice_yfinance.api_test_func,  "args" : insert_data}
    # ]

    # 심볼 리스트를 50개씩 나누기
    batch_size = 10  # 한 번에 처리할 심볼 수
    symbol_batches = list(chunk_list(symbols, batch_size))

    # 각 배치별로 func_list 생성
    func_list = []
    for symbol_batch in symbol_batches:
        func_list.append({
            "func": api_stockprice_yfinance.api_test_func,
            "args": [symbol_batch]  # 각 배치를 리스트로 감싸서 전달
        })

    # 각 func에 대해 스케줄 등록
    for index, func in enumerate(func_list):
        # 각 배치마다 시작 시간을 조금씩 다르게 설정 (30초 간격)
        start_date = datetime.now() + timedelta(seconds=30 * index)
        
        scheduler.add_job(
            register_job_with_mongo,
            trigger='interval',
            seconds=180,  # 3분마다 반복
            start_date=start_date,  # 시작 시간을 다르게 설정
            coalesce=True,
            max_instances=1,
            id=f"{func['func'].__name__}_{index}",  # 기존 ID 형식 유지
            args=[client, ip_add, db_name, col_name, func['func'], func['args']]  # 기존 args 구조 유지
        )
        print(f"Scheduled {func['func'].__name__} batch {index} with {len(func['args'][0])} symbols, starting at {start_date}")

    # 실패한 작업을 다시 시도하는 로직
    def retry_failed_jobs():
        print("Checking for skipped jobs...")
        for job in scheduler.get_jobs():
            if job.id.startswith(f"{api_stockprice_yfinance.api_test_func.__name__}"):
                try:
                    # 작업 상태 확인
                    if job.next_run_time is not None:
                        print(f"Checking job {job.id}")
                        # 작업을 즉시 실행
                        retry_id = f"retry_{job.id}_{int(time.time())}"
                        scheduler.add_job(
                            register_job_with_mongo,
                            trigger='date',  # 즉시 실행
                            id=retry_id,
                            args=job.args,
                            replace_existing=True
                        )
                except Exception as e:
                    print(f"Error checking job {job.id}: {e}")

    # 실패한 작업을 주기적으로 재시도
    scheduler.add_job(
        retry_failed_jobs, 
        trigger='interval', 
        seconds=60,  # 1분마다 확인
        id='retry_job_checker'
    )

    
    scheduler.start()

    while True:

        pass
    
    return True


if __name__ == '__main__':
    run()
    pass
