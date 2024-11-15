import yfinance as yf
import pandas as pd

# get_market_senti 함수 정의
def get_market_senti(symbol):
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

    # 결과 출력
    print(f"RSI ({symbol} - 최근 1년 월간 데이터):")
    print(rsi.dropna())  # NaN을 제외하고 출력

    print("\nMACD (최근 1년 월간 데이터):")
    print("MACD Line:")
    print(macd_line.dropna())
    print("Signal Line:")
    print(signal_line.dropna())
    print("MACD Histogram:")
    print(macd_histogram.dropna())

# 예시로 삼성전자의 데이터를 출력
get_market_senti("005930.KS")
