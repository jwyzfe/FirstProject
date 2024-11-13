

'''

'''
# 공통 부분을 import 하여 구현

# 직접 만든 class 
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from commons.api_send_requester import ApiRequester
import yfinance as yf

# 직접 만든 class 
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from commons.mongo_insert_recode import connect_mongo

# mongo DB 동작
from pymongo import MongoClient

'''
        # show meta information about the history (requires history() to be called first)
        msft.history_metadata

        # show actions (dividends, splits, capital gains)
        msft.actions
        msft.dividends
        msft.splits
        msft.capital_gains  # only for mutual funds & etfs

        # show share count
        msft.get_shares_full(start="2022-01-01", end=None)

        # show financials:
        msft.calendar
        msft.sec_filings
        # - income statement
        msft.income_stmt
        msft.quarterly_income_stmt
        # - balance sheet
        msft.balance_sheet
        msft.quarterly_balance_sheet
        # - cash flow statement
        msft.cashflow
        msft.quarterly_cashflow
        # see `Ticker.get_income_stmt()` for more options

        # show holders
        msft.major_holders
        msft.institutional_holders
        msft.mutualfund_holders
        msft.insider_transactions
        msft.insider_purchases
        msft.insider_roster_holders

        msft.sustainability

        # show recommendations
        msft.recommendations
        msft.recommendations_summary
        msft.upgrades_downgrades

        # show analysts data
        msft.analyst_price_targets
        msft.earnings_estimate
        msft.revenue_estimate
        msft.earnings_history
        msft.eps_trend
        msft.eps_revisions
        msft.growth_estimates

        # Show future and historic earnings dates, returns at most next 4 quarters and last 8 quarters by default.
        # Note: If more are needed use msft.get_earnings_dates(limit=XX) with increased limit argument.
        msft.earnings_dates

        # show ISIN code - *experimental*
        # ISIN = International Securities Identification Number
        msft.isin

        # show options expirations
        msft.options

        # show news
        msft.news

        # get option chain for specific expiration
        opt = msft.option_chain('YYYY-MM-DD')
        # data available via: opt.calls, opt.puts

'''
'''
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

'''
class api_stockprice_yfinance:
    def api_test_func(symbol_list):
        return_histlist = []
        for symbol in symbol_list[:2]:
            msft = yf.Ticker(symbol) # "MSFT"
            # get all stock info
            msft.info

            # get historical market data
            hist = msft.history(period="max")
            if hist is not None and not hist.empty:  # hist가 None이 아니고 비어있지 않은 경우
                hist['symbol'] = symbol  # 'symbol' 컬럼 추가
                return_histlist.append(hist)
        # print(hist)

        return return_histlist

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
    result_list = api_stockprice_yfinance.api_test_func(symbol_list=symbols)

    # 스케쥴러 등록 
    # mongodb 가져올 수 있도록
    ip_add = f'mongodb://localhost:27017/'
    db_name = f'DB_SGMN' # db name 바꾸기
    col_name = f'collection_name' # collection name 바꾸기

    # MongoDB 서버에 연결 
    client = MongoClient(ip_add) 

    connect_mongo.insert_recode_in_mongo(client, db_name, col_name, result_list)

    pass