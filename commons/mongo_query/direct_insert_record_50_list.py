# main에서 다른 폴더 경로 참조하려면 필요
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 직접 만든 class    
from config_reader import read_config  # config read 용       
from mongo_insert_recode import connect_mongo as connect_mongo_insert
from mongo_find_recode import connect_mongo as connect_mongo_find

# mongo DB
from pymongo import MongoClient
import pandas as pd

def make_direct_insert_work_recode(df):
    work_records = []
    
    for index, row in df.iterrows():
        symbol = str(row['symbol']).zfill(6)  # 6자리로 맞추기
        symbol_market = f"{symbol}.KS"  # .KS 붙이기
        corp_name = row['corp_name']
        print(corp_name)
        work_record = {
            'symbol': symbol,
            'symbol_market': symbol_market,
            'corp_name': corp_name,
            # 'iswork': 'ready'
        }
        work_records.append(work_record)
    
    return work_records
    

if __name__ == '__main__':

    config = read_config()
    ip_add = config['MongoDB_remote']['ip_add']
    db_name = config['MongoDB_remote']['db_name']
    mongo_client = MongoClient(ip_add)

    db_name = 'DB_SGMN'
    col_name_work = 'COL_NAS25_KOSPI25_CORPLIST'

    corp_list_dict = {
        "market": ["nasdaq"] * 25 + ["kospi"] * 25,
        "symbol": [
            "NVDA", "AAPL", "MSFT", "AMZN", "GOOG", 
            "META", "TSLA", "BRK-B", "AVGO", "WMT", 
            "JPM", "LLY", "V", "UNH", "XOM", 
            "ORCL", "MA", "COST", "HD", "PG", 
            "NFLX", "JNJ", "BAC", "CRM", "ABBV", 
            "005930.KS", "000660.KS", "373220.KS", "207940.KS", 
            "HYMTF", "000270.KS", "KB", "068270.KS", 
            "035420.KS", "SHG", "051910.KS", "PKX", 
            "012330.KS", "032830.KS", "006405.KS", "096775.KS", 
            "086790.KS", "196170.KQ", "011200.KS", "010130.KS", 
            "066570.KS", "012450.KS", "035720.KS", "KEP", 
            "003670.KS"
        ],
        "symbol_ks": [''] * 25 + [
            "005930", "000660", "373220", "207940", 
            "HYMTF", "000270", "KB", "068270", 
            "035420", "SHG", "051910", "PKX", 
            "012330", "032830", "006405", "096775", 
            "086790", "196170", "011200", "010130", 
            "066570", "012450", "035720", "KEP", 
            "003670"
        ],
    }

    df_record = pd.DataFrame(corp_list_dict)

    connect_mongo_insert.insert_recode_in_mongo_notime(
        mongo_client, 
        db_name, 
        col_name_work, 
        df_record
    )
 
    mongo_client.close()