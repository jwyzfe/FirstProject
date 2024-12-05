import yfinance as yf
import pandas as pd
# datetime
from datetime import datetime 
import pytz

class calc_market_senti:
# get_market_senti 함수 정의
    @classmethod
    def get_market_senti(cls, symbol):
        try:
            # 2년 간의 데이터 가져오기
            stock_data = yf.download(symbol, period="2y", interval="1mo")

            # 최근 1년간의 데이터만 선택
            last_year_data = stock_data.loc[stock_data.index >= (stock_data.index[-1] - pd.DateOffset(years=1))]

            # 종가 데이터
            closing_prices = last_year_data['Close']

            # 1. RSI 계산 함수
            def calculate_rsi(data, period=14):
                delta = data.diff()  # 종가 변화량
                gain = delta.where(delta > 0, 0)  # 상승분
                loss = -delta.where(delta < 0, 0)  # 하락분
                avg_gain = gain.rolling(window=period, min_periods=1).mean()  # 평균 상승분
                avg_loss = loss.rolling(window=period, min_periods=1).mean()  # 평균 하락분
                rs = avg_gain / avg_loss  # 상대 강도
                rsi = 100 - (100 / (1 + rs))
                return rsi

            # 2. MACD 계산 함수
            def calculate_macd(data, short_window=12, long_window=26, signal_window=9):
                ema_short = data.ewm(span=short_window, adjust=False).mean()  # 12일 EMA
                ema_long = data.ewm(span=long_window, adjust=False).mean()  # 26일 EMA
                macd_line = ema_short - ema_long  # MACD 선
                signal_line = macd_line.ewm(span=signal_window, adjust=False).mean()  # 신호선
                macd_histogram = macd_line - signal_line  # 히스토그램
                return macd_line, signal_line, macd_histogram

            # RSI 및 MACD 계산
            rsi = calculate_rsi(closing_prices)
            macd_line, signal_line, macd_histogram = calculate_macd(closing_prices)

            # DataFrame을 원하는 형태로 변환하는 함수
            def transform_df(df):
                # DataFrame 리셋 (index를 column으로 변환)
                column = df.columns[0]
                df = df.reset_index()
                
                # 'Ticker'와 'Date' 컬럼만 남기고, 'Value' 컬럼의 이름을 변경
                df.columns = ['Date', 'Value']
                df['Ticker'] = column
                
                return df[['Ticker', 'Date', 'Value']]

            # 4개의 DataFrame이 있다고 가정 (df1, df2, df3, df4)
            # df1 = 첫 번째 DataFrame
            # df2 = 두 번째 DataFrame
            # df3 = 세 번째 DataFrame
            # df4 = 네 번째 DataFrame

            # 각 DataFrame을 변환
            df1_transformed = transform_df(rsi)
            df2_transformed = transform_df(macd_line)
            df3_transformed = transform_df(signal_line)
            df4_transformed = transform_df(macd_histogram)

            # DataFrame 병합
            merged_df = df1_transformed.merge(df2_transformed, on=['Ticker', 'Date'], suffixes=('_rsi', '_macd_line')) \
                .merge(df3_transformed, on=['Ticker', 'Date']) \
                .merge(df4_transformed, on=['Ticker', 'Date'], suffixes=('_signal_line', '_macd_histogram'))

            # 컬럼 이름 변경
            merged_df.columns = ['Ticker', 'Date', 'rsi', 'macd_line', 'signal_line', 'macd_histogram']

            return merged_df
        except Exception as e:
            print(e)
            return pd.DataFrame()
        # # 결과 출력
        # print(merged_df)
        # pass
        # # 결과 출력
        # print(f"RSI ({symbol} - 최근 1년 월간 데이터):")
        # print(rsi.dropna())  # NaN을 제외하고 출력

        # print("\nMACD (최근 1년 월간 데이터):")
        # print("MACD Line:")
        # print(macd_line.dropna())
        # print("Signal Line:")  
        # print(signal_line.dropna())
        # print("MACD Histogram:")
        # print(macd_histogram.dropna())
    
    @classmethod
    def get_market_senti_list(cls,symbol_list):
        return_histlist = pd.DataFrame()  # 빈 DataFrame 생성
        print(f"start:{datetime.now(pytz.timezone('Asia/Seoul'))}")

        for symbol in symbol_list:
            merged_df = cls.get_market_senti(symbol)
            # merged_df가 비어 있으면 다음 반복으로 넘어감
            if merged_df.empty:
                print(f"merged_df is empty for symbol: {symbol}, skipping...")
                continue
            return_histlist = pd.concat([return_histlist, merged_df])

        print(f"loop:{datetime.now(pytz.timezone('Asia/Seoul'))}")

        return return_histlist
        

if __name__ == '__main__' :
    # 예시로 삼성전자의 데이터를 출력
    # calc_market_senti.get_market_senti("005930.KS")
    symbols = [
    "NVDA", "AAPL", "MSFT", "AMZN", "META", "GOOGL", "TSLA", "GOOG", "AVGO"
      ]
    calc_market_senti.get_market_senti_list(symbols)
