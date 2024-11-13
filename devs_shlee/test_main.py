
# 스크래퍼 import 
from apscheduler.schedulers.background import BackgroundScheduler
# mongo DB 동작
from pymongo import MongoClient
# time.sleep
import time

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

def run():

    # 스케쥴러 등록 
    # mongodb 가져올 수 있도록
    ip_add = f'mongodb://localhost:27017/'
    db_name = f'DB_NAME' # db name 바꾸기
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
    insert_data = [symbols] # [val1,val2,val3]
    
    func_list = [
        # {"func" : bs4_scrapping.do_scrapping, "args" : insert_data},
        {"func" : api_stockprice_yfinance.api_test_func,  "args" : insert_data}
    ]

    for func in func_list:
        scheduler.add_job(register_job_with_mongo,                         
                        trigger='interval',
                        seconds=30, # 5초 마다 반복 
                        coalesce=True, 
                        max_instances=1,
                        id=func['func'].__name__, # 독립적인 함수 이름 주어야 함.
                        # args=[args_list]
                        args=[client, ip_add, db_name, col_name, func['func'], func['args']] # 
                        )
    
    
    scheduler.start()

    while True:

        pass
    
    return True


if __name__ == '__main__':
    run()
    pass
