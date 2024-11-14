

# mongo DB 동작
from pymongo import MongoClient
# yfinance
import yfinance as yf
# padas
import pandas as pd
# datetime
from datetime import datetime 

# 직접 만든 class 
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from commons.api_send_requester import ApiRequester
from commons.mongo_find_recode import connect_mongo as connect_mongo_find
from commons.mongo_insert_recode import connect_mongo as connect_mongo_insert


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
class api_stockprice_yfinance:
    def api_test_func(symbol_list):
        return_histlist = pd.DataFrame()  # 빈 DataFrame 생성
        print(f"start:{datetime.now()}")
        for symbol in symbol_list:
            msft = yf.Ticker(symbol) # "MSFT"
            # get all stock info
            msft.info
            # get historical market data
            hist = msft.history(period="max")
            if hist is not None and not hist.empty:  # hist가 None이 아니고 비어있지 않은 경우
                hist['symbol'] = symbol  # 'symbol' 컬럼 추가
                # 인덱스를 'Date' 컬럼으로 변환
                df_with_date = hist.reset_index()
                # DataFrame을 레코드 리스트로 변환 후 머지
                return_histlist = pd.concat([return_histlist, df_with_date])
            print(f"loop:{datetime.now()}")
        # print(hist)

        return return_histlist

if __name__ == "__main__":

    # 스케쥴러 등록 
    # mongodb 가져올 수 있도록
    ip_add = f'mongodb://192.168.0.91:27017/'
    db_name = f'DB_SGMN'
    col_name = f'COL_SYMBOL_RECORD' # 데이터 읽을 collection

    # MongoDB 서버에 연결 
    client = MongoClient(ip_add)

    symbols = connect_mongo_find.read_records(client, db_name, col_name)

    result_list = api_stockprice_yfinance.api_test_func(symbol_list=symbols[:1])

    col_name = f'COL_STOCK_PRICE_HISTORY' # 데이터 읽을 collection
    connect_mongo_insert.insert_recode_in_mongo(client, db_name, col_name, result_list)

    pass