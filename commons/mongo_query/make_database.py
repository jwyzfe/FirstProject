
# mongo DB 동작
from pymongo import MongoClient
# 직접 만든 class 
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from mongo_insert_recode import connect_mongo


if __name__ == "__main__":
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
    # result_list = api_stockprice_yfinance.api_test_func(symbol_list=symbols)

    # 스케쥴러 등록 
    # mongodb 가져올 수 있도록
    ip_add = f'mongodb://192.168.0.50:27017/'
    db_name = f'DB_SGMN'
    col_name = f'COL_AMERICA_CORPLIST' # collection name 바꾸기

    # MongoDB 서버에 연결 
    client = MongoClient(ip_add) 
    # 심볼 리스트를 MongoDB에 삽입
    # 각 심볼을 딕셔너리 형태로 변환
    records = [{"symbol": symbol} for symbol in symbols]

    connect_mongo.insert_recode_in_mongo(client, db_name, col_name, records)

    # connect_mongo.insert_recode_in_mongo(client, db_name, col_name, result_list)

    pass