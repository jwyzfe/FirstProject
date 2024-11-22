import requests
from pandas_datareader import data as pdr
import matplotlib.pyplot as plt
import yfinance
import pandas as pd


# [1] Money Flow Index
# https://skyeong.net/282
def calculate_mfi(data, period=14):
    """
    MFI(Money Flow Index) 계산 함수
    
    Parameters:
    data (pandas.DataFrame): 주가 데이터 (high, low, close, volume 필요)
    period (int): 계산 기간 (기본값 14일)
    
    Returns:
    pandas.Series: MFI 값
    """
    # 대표 주가(Typical Price) 계산
    typical_price = (data['HIGH'] + data['LOW'] + data['CLOSE']) / 3
    
    # Money Flow 계산
    money_flow = typical_price * data['VOLUME']
    
    # Positive/Negative Money Flow 구분
    price_diff = typical_price.diff()
    positive_flow = pd.Series(0.0, index=typical_price.index)
    negative_flow = pd.Series(0.0, index=typical_price.index)
    
    # 가격 상승시 positive_flow, 하락시 negative_flow
    positive_flow[price_diff > 0] = money_flow[price_diff > 0]
    negative_flow[price_diff < 0] = money_flow[price_diff < 0]
    
    # n기간 동안의 positive/negative money flow 합계
    positive_mf = positive_flow.rolling(window=period).sum()
    negative_mf = negative_flow.rolling(window=period).sum()
    
    # MFI 계산
    mfi = 100 * (positive_mf / (positive_mf + negative_mf))
    
    return mfi

# 사용 예시:
# mfi = calculate_mfi(df)

# [2] Relative Strength Index
#전일 가격 대비 오늘 가격이 몇퍼센트 오른 것인지 확인하는 지표
# https://www.investopedia.com/terms/r/rsi.asp

def calculate_rsi(data, period=14):
    """
    RSI(Relative Strength Index) 계산 함수
    
    Parameters:
    data (pandas.DataFrame): 주가 데이터 (Close 가격 필요)
    period (int): RSI 계산 기간 (기본값 14일)
    
    Returns:
    pandas.Series: RSI 값
    """
    # 일일 가격 변화 계산
    delta = data['CLOSE'].diff()
    
    # 상승분(U)과 하락분(D) 분리
    gain = (delta.where(delta > 0, 0))
    loss = (-delta.where(delta < 0, 0))
    
    # 첫 평균 계산
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    
    # 그 이후 평균 계산
    for i in range(period, len(delta)):
        avg_gain.iloc[i] = (avg_gain.iloc[i-1] * (period-1) + gain.iloc[i]) / period
        avg_loss.iloc[i] = (avg_loss.iloc[i-1] * (period-1) + loss.iloc[i]) / period
    
    # RS(Relative Strength) 계산
    rs = avg_gain / avg_loss
    
    # RSI 계산
    rsi = 100 - (100 / (1 + rs))
    
    return rsi

# [3] Force Index
# 거래량과 가격의 변화를 곱한 값
# https://alphasquare.co.kr/home/insight/posts/abd02bf6-faca-416c-81b6-14ff5c4d2bf4

# Load the necessary packages and modules


# Force Index 
def ForceIndex(data, ndays): 
    FI = pd.Series(data['CLOSE'].diff(ndays) * data['VOLUME'], name = 'ForceIndex') 
    data = data.join(FI) 
    return data


# Retrieve the Apple Inc. data from Yahoo finance:
data = pdr.get_data_yahoo("AAPL", start="2010-01-01", end="2016-01-01") 
data = pd.DataFrame(data)

# Compute the Force Index for AAPL
n = 1
AAPL_ForceIndex = ForceIndex(data,n)
print(AAPL_ForceIndex)

def calculate_force_index(data, period=1):
    """
    Force Index 계산 함수
    
    Parameters:
    data (pandas.DataFrame): 주가 데이터 (Close 가격과 Volume 필요)
    period (int): 기간 (기본값 1일)
    
    Returns:
    pandas.Series: Force Index 값
    """
    # Force Index = 가격 변화 * 거래량
    force_index = data['CLOSE'].diff(period) * data['VOLUME']
    
    return force_index

# EMA를 적용한 Force Index도 계산 가능한 함수 선택사항
# https://www.investopedia.com/articles/trading/03/031203.asp <Interpreting the Force Index>

def calculate_force_index_ema(data, period=1, ema_period=13):
    """
    Force Index에 EMA를 적용한 계산 함수
    
    Parameters:
    data (pandas.DataFrame): 주가 데이터 (Close 가격과 Volume 필요)
    period (int): 가격 변화 기간 (기본값 1일)
    ema_period (int): EMA 기간 (기본값 13일)
    
    Returns:
    pandas.Series: Force Index EMA 값
    """
    # 기본 Force Index 계산
    force_index = data['CLOSE'].diff(period) * data['VOLUME']
    
    # EMA 적용
    force_index_ema = force_index.ewm(span=ema_period, adjust=False).mean()
    
    return force_index_ema

# [4] Stochastic Oscillator
# Fast Stochastic Oscillator, Slow Stochastic Oscillator
# https://wendys.tistory.com/176
# https://entreprenerdly.com/top-6-momentum-indicators-in-python/

#Fast %K = ((현재가 - n기간 중 최저가) / (n기간 중 최고가 - n기간 중 최저가)) * 100
def get_stochastic_fast_k(close_price, low, high, n=5): 
    fast_k = ((close_price - low.rolling(n).min()) / (high.rolling(n).max() - low.rolling(n).min())) * 100  
    return fast_k

# Slow %K = Fast %K의 m기간 이동평균(SMA)
def get_stochastic_slow_k(fast_k, n=3): 
    slow_k = fast_k.rolling(n).mean()  
    return slow_k

# Slow %D = Slow %K의 t기간 이동평균(SMA)
def get_stochastic_slow_d(slow_k, n=3): 
    slow_d = slow_k.rolling(n).mean()  
    return slow_d

# fast_k, slow_k, slow_d를 획득

df['fast_k'] = get_stochastic_fast_k(df['CLOSE'], df['LOW'], df['HIGH'], 5)
df['slow_k'] = get_stochastic_slow_k(df['fast_k'], 3)
df['slow_d'] = get_stochastic_slow_d(df['slow_k'], 3)

#[5] Ultimate Oscillator
# 최근 N일 동안의 최고가, 최저가, 종가를 사용하여 계산
# https://www.investopedia.com/terms/u/ultimateoscillator.asp
# https://altfins.com/knowledge-base/stochastic-rsi-fast-3-3-14-14/

def get_stochastic_oscillator(data, k_period=14, d_period=3):
    """
    Stochastic Oscillator 계산 함수
    """
    # Fast %K 계산
    data['fast_k'] = get_stochastic_fast_k(data['CLOSE'], data['LOW'], data['HIGH'], k_period)
    # Slow %K 계산
    data['slow_k'] = get_stochastic_slow_k(data['fast_k'], d_period)
    # Slow %D 계산
    data['slow_d'] = get_stochastic_slow_d(data['slow_k'], d_period)
    return data


#[6] True Strength Index
# 최근 N일 동안의 최고가, 최저가, 종가를 사용하여 계산
# https://www.quantifiedstrategies.com/true-strength-index/

def calculate_tsi(data, long_period=25, short_period=13):
    """
    TSI(True Strength Index) 계산 함수
    
    Parameters:
    data (pandas.DataFrame): 주가 데이터 (Close 가격 필요)
    long_period (int): 긴 EMA 기간 (기본값 25일)
    short_period (int): 짧은 EMA 기간 (기본값 13일)
    
    Returns:
    pandas.Series: TSI 값
    """
    # Step 1: 가격 변화량 계산
    price_change = data['CLOSE'].diff()
    
    # 가격 변화량의 절대값 계산
    abs_price_change = price_change.abs()
    
    # Step 2: 첫 번째 EMA 계산 (25일)
    smoothed_pc = price_change.ewm(span=long_period, adjust=False).mean()
    smoothed_abs_pc = abs_price_change.ewm(span=long_period, adjust=False).mean()
    
    # Step 3: 두 번째 EMA 계산 (13일)
    double_smoothed_pc = smoothed_pc.ewm(span=short_period, adjust=False).mean()
    double_smoothed_abs_pc = smoothed_abs_pc.ewm(span=short_period, adjust=False).mean()
    
    # Step 4: TSI 계산
    tsi = 100 * (double_smoothed_pc / double_smoothed_abs_pc)
    
    return tsi

# 사용 예시:
# tsi = calculate_tsi(df)

#[7] Williams %R
# 최근 N일 동안의 최고가, 최저가, 종가를 사용하여 계산
# https://www.investopedia.com/terms/w/williamsr.asp
# https://www.insightbig.com/post/increasing-stock-returns-by-combining-williams-r-and-macd-in-python


def calculate_williams_r(data, period=14):
    """
    Williams %R 지표 계산 함수
    
    Parameters:
    data (pandas.DataFrame): 주가 데이터 (high, low, close 가격 필요)
    period (int): 계산 기간 (기본값 14일)
    
    Returns:
    pandas.Series: Williams %R 값
    """
    # 최근 N일 최고가
    highest_high = data['HIGH'].rolling(window=period).max()
    
    # 최근 N일 최저가
    lowest_low = data['LOW'].rolling(window=period).min()
    
    # Williams %R 계산: (최고가 - 현재가) / (최고가 - 최저가) * -100
    williams_r = ((highest_high - data['CLOSE']) / (highest_high - lowest_low)) * -100
    
    return williams_r

# 사용 예시:
# williams_r = calculate_williams_r(df)


