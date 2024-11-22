import pandas as pd
import numpy as np
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')  # 로컬 서버 연결 (포트 번호는 기본값 27017)
db = client['DB_SGMN']  # 데이터베이스 선택
collection = db['COL_STOCKPRICE_HISTORY']  # 컬렉션 선택

data = collection.find()


def calculate_adx(high, low, close, lookback_periods=14):
    """
    ADX(Average Directional Movement Index)를 계산하는 함수.

    Parameters:
        high (pd.Series): 고가 데이터
        low (pd.Series): 저가 데이터
        close (pd.Series): 종가 데이터
        lookback_periods (int): ADX 계산에 사용할 기간, 기본값은 14
    
    Returns:
        pd.DataFrame: +DI, -DI, DX, ADX가 포함된 데이터프레임
    """
    # 1. 방향성 이동 계산
    plus_dm = high.diff()
    minus_dm = low.diff()

    plus_dm[plus_dm < 0] = 0  # 양의 방향성 이동
    minus_dm[minus_dm > 0] = 0  # 음의 방향성 이동

    # 2. True Range (TR) 계산
    high_low = high - low
    high_close_prev = (high - close.shift()).abs()
    low_close_prev = (low - close.shift()).abs()
    true_range = pd.concat([high_low, high_close_prev, low_close_prev], axis=1).max(axis=1)

    # 3. Smoothed values (14일 동안의 이동평균)
    smoothed_tr = true_range.rolling(window=lookback_periods).mean()
    smoothed_plus_dm = plus_dm.rolling(window=lookback_periods).mean()
    smoothed_minus_dm = minus_dm.abs().rolling(window=lookback_periods).mean()

    # 4. +DI, -DI 계산
    plus_di = (smoothed_plus_dm / smoothed_tr) * 100
    minus_di = (smoothed_minus_dm / smoothed_tr) * 100

    # 5. DX (Directional Movement Index) 계산
    dx = (np.abs(plus_di - minus_di) / (plus_di + minus_di)) * 100

    # 6. ADX 계산 (DX의 평균)
    adx = dx.rolling(window=lookback_periods).mean()

    # 결과 데이터프레임 생성
    adx_df = pd.DataFrame({
        'Plus_DI': plus_di,
        'Minus_DI': minus_di,
        'DX': dx,
        'ADX': adx
    })

    return adx_df
# ADX의 값이 0에 가까울수록 약한 추세, 100에 가까울수록 강한 추세로 판단할 수 있다.

'''
def CCI_indicator(high, low, close):
    



def calculate_cci(high, low, close, lookback_periods=20):
    """
    CCI(Commodity Channel Index)를 계산하는 함수.

    Parameters:
        high (pd.Series): 고가 데이터
        low (pd.Series): 저가 데이터
        close (pd.Series): 종가 데이터
        lookback_periods (int): CCI 계산에 사용할 기간, 기본값은 20

    Returns:
        pd.Series: CCI 값
    """
    # 1. Typical Price (TP) 계산
    typical_price = (high + low + close) / 3

    # 2. 이동평균 (SMA) 계산
    moving_average = typical_price.rolling(window=lookback_periods).mean()

    # 3. Mean Deviation 계산
    mean_deviation = typical_price.rolling(window=lookback_periods).apply(
        lambda x: pd.Series(x).mad(), raw=True
    )

    # 4. CCI 계산
    cci = (typical_price - moving_average) / (0.015 * mean_deviation)

    return cci



def calculate_dpo(prices: pd.Series, short_period: int = 12, long_period: int = 26) -> pd.Series:
    """
    Differential Price Oscillator (DPO) 계산 함수
    
    Parameters:
    prices (pd.Series): 종목의 가격 데이터 (예: 종가)
    short_period (int): 단기 이동 평균 기간 (기본값: 12)
    long_period (int): 장기 이동 평균 기간 (기본값: 26)
    
    Returns:
    pd.Series: DPO 값
    """
    # 단기 및 장기 이동 평균 계산
    short_ma = prices.rolling(window=short_period).mean()
    long_ma = prices.rolling(window=long_period).mean()
    
    # DPO 계산 (단기 이동 평균 - 장기 이동 평균)
    dpo = short_ma - long_ma
    
    return dpo

# 예시 가격 데이터 (가격 데이터가 pandas Series 형식으로 있다고 가정)
# 예시로, 가격 데이터를 pandas Series로 설정
prices = pd.Series([10, 11, 12, 13, 14, 13, 15, 16, 17, 18, 19, 20, 21, 22, 23])

# DPO 계산
dpo_values = calculate_dpo(prices)

# 결과 출력
print(dpo_values)





# 주식 데이터 (종가가 포함된 DataFrame)
def calculate_roc(prices, period):
    """가격 변화율 (Rate of Change, ROC) 계산"""
    return (prices.diff(periods=period) / prices.shift(periods=period)) * 100

def weighted_moving_average(series, period):
    """가중 이동 평균(WMA) 계산"""
    weights = pd.Series(range(1, period + 1))
    return series.rolling(window=period).apply(lambda x: (x * weights).sum() / weights.sum(), raw=True)

def calculate_kst(prices):
    """KST 지표 계산"""
    # ROC 계산 (4개의 기간에 대해)
    roc1 = calculate_roc(prices, 10)  # 10일 ROC
    roc2 = calculate_roc(prices, 15)  # 15일 ROC
    roc3 = calculate_roc(prices, 20)  # 20일 ROC
    roc4 = calculate_roc(prices, 30)  # 30일 ROC

    # WMA를 각각의 ROC에 적용
    wma_roc1 = weighted_moving_average(roc1, 10)
    wma_roc2 = weighted_moving_average(roc2, 10)
    wma_roc3 = weighted_moving_average(roc3, 10)
    wma_roc4 = weighted_moving_average(roc4, 15)

    # KST 지표 계산
    kst = wma_roc1 + wma_roc2 + wma_roc3 + wma_roc4
    return kst

# 예시 사용법
if __name__ == "__main__":
    # 예시 종가 데이터 (실제로는 주식 데이터를 사용)
    data = {
        'Date': pd.date_range(start="2023-01-01", periods=100),
        'Close': [i + (i * 0.02) for i in range(100)]  # 임시로 상승하는 데이터
    }
    df = pd.DataFrame(data)
    
    # KST 지표 계산
    df['KST'] = calculate_kst(df['Close'])
    
    # 결과 출력
    print(df[['Date', 'Close', 'KST']].tail())  # 마지막 5개의 데이터 출력





def calculate_ichimoku(data):
    """
    일목균형표 계산 함수.
    :param data: pandas DataFrame, 반드시 'High', 'Low', 'Close' 컬럼이 포함되어야 함.
    :return: DataFrame에 일목균형표 컬럼 추가
    """
    # 전환선 (Tenkan-sen)
    data['Tenkan-sen'] = (
        (data['High'].rolling(window=9).max() + data['Low'].rolling(window=9).min()) / 2
    )
    
    # 기준선 (Kijun-sen)
    data['Kijun-sen'] = (
        (data['High'].rolling(window=26).max() + data['Low'].rolling(window=26).min()) / 2
    )
    
    # 선행스팬1 (Senkou Span A)
    data['Senkou Span A'] = (
        (data['Tenkan-sen'] + data['Kijun-sen']) / 2
    ).shift(26)
    
    # 선행스팬2 (Senkou Span B)
    data['Senkou Span B'] = (
        (data['High'].rolling(window=52).max() + data['Low'].rolling(window=52).min()) / 2
    ).shift(26)
    
    # 후행스팬 (Chikou Span)
    data['Chikou Span'] = data['Close'].shift(-26)
    
    return data

# 예제 데이터
if __name__ == "__main__":
    # 가상 데이터 생성
    data = {
        'Date': pd.date_range(start="2023-01-01", periods=100),
        'High': [100 + i * 0.5 for i in range(100)],
        'Low': [90 + i * 0.5 for i in range(100)],
        'Close': [95 + i * 0.5 for i in range(100)]
    }
    df = pd.DataFrame(data)
    
    # 일목균형표 계산
    df = calculate_ichimoku(df)
    
    # 결과 출력
    print(df[['Date', 'Tenkan-sen', 'Kijun-sen', 'Senkou Span A', 'Senkou Span B', 'Chikou Span']].tail())




def calculate_sma(data, window):
    """
    단순 이동평균(SMA) 계산
    :param data: pandas DataFrame, 반드시 'Close' 컬럼이 포함되어야 함.
    :param window: 이동평균을 계산할 기간 (int)
    :return: SMA가 추가된 DataFrame
    """
    data[f'SMA_{window}'] = data['Close'].rolling(window=window).mean()
    return data

# 예제 데이터
if __name__ == "__main__":
    data = {
        'Date': pd.date_range(start="2023-01-01", periods=20),
        'Close': [100 + i for i in range(20)]
    }
    df = pd.DataFrame(data)

    # SMA 계산 (5일 이동평균)
    df = calculate_sma(df, window=5)
    print(df)




def calculate_cma(data):
    """
    누적 이동평균(CMA) 계산
    :param data: pandas DataFrame, 반드시 'Close' 컬럼이 포함되어야 함.
    :return: CMA가 추가된 DataFrame
    """
    data['CMA'] = data['Close'].expanding().mean()
    return data

# 예제 데이터
if __name__ == "__main__":
    data = {
        'Date': pd.date_range(start="2023-01-01", periods=10),
        'Close': [100, 105, 110, 120, 115, 125, 130, 135, 140, 145]
    }
    df = pd.DataFrame(data)

    # CMA 계산
    df = calculate_cma(df)
    print(df)




def calculate_mass_index(data, ema_period=9, sum_period=25):
    """
    Mass Index(MI) 계산
    :param data: pandas DataFrame, 반드시 'High'와 'Low' 컬럼 포함
    :param ema_period: EMA 계산에 사용할 기간 (기본값: 9)
    :param sum_period: MI 누적합 계산에 사용할 기간 (기본값: 25)
    :return: Mass Index가 추가된 DataFrame
    """
    # 1. 범위 계산
    data['Range'] = data['High'] - data['Low']

    # 2. EMA(범위, N) 계산
    data['EMA1'] = data['Range'].ewm(span=ema_period, adjust=False).mean()

    # 3. EMA(EMA(범위, N), N) 계산
    data['EMA2'] = data['EMA1'].ewm(span=ema_period, adjust=False).mean()

    # 4. EMA 비율 계산
    data['EMA_Ratio'] = data['EMA1'] / data['EMA2']

    # 5. Mass Index 계산 (누적합)
    data['Mass Index'] = data['EMA_Ratio'].rolling(window=sum_period).sum()

    # 불필요한 중간 계산 컬럼 제거
    return data[['High', 'Low', 'Mass Index']]

# 예제 데이터
if __name__ == "__main__":
    data = {
        'Date': pd.date_range(start="2023-01-01", periods=30),
        'High': [100 + i for i in range(30)],
        'Low': [90 + i for i in range(30)],
    }
    df = pd.DataFrame(data)

    # Mass Index 계산
    df = calculate_mass_index(df)
    print(df)





def calculate_parabolic_sar(data, af_start=0.02, af_step=0.02, af_max=0.2):
    """
    파라볼릭 SAR 계산
    :param data: pandas DataFrame, 반드시 'High', 'Low' 컬럼 포함
    :param af_start: 초기 가속 계수 (기본값: 0.02)
    :param af_step: 가속 계수 증가폭 (기본값: 0.02)
    :param af_max: 가속 계수 최대값 (기본값: 0.2)
    :return: Parabolic SAR이 추가된 DataFrame
    """
    high = data['High']
    low = data['Low']
    sar = []
    trend = []
    af = af_start
    ep = high[0]
    prev_sar = low[0]
    is_uptrend = True  # 초기 추세는 상승 추세로 가정

    for i in range(len(data)):
        if is_uptrend:
            # 상승 추세
            sar.append(prev_sar + af * (ep - prev_sar))
            ep = max(ep, high[i])  # 최고가 갱신
            if high[i] < sar[-1]:  # 하락 추세로 전환
                is_uptrend = False
                sar[-1] = ep  # 반전 시 초기 SAR 설정
                ep = low[i]  # 반전 시 최저가 갱신
                af = af_start
        else:
            # 하락 추세
            sar.append(prev_sar - af * (prev_sar - ep))
            ep = min(ep, low[i])  # 최저가 갱신
            if low[i] > sar[-1]:  # 상승 추세로 전환
                is_uptrend = True
                sar[-1] = ep  # 반전 시 초기 SAR 설정
                ep = high[i]  # 반전 시 최고가 갱신
                af = af_start
        
        # SAR 갱신
        prev_sar = sar[-1]
        af = min(af + af_step, af_max) if ep != prev_sar else af  # 가속 계수 증가
        trend.append("Uptrend" if is_uptrend else "Downtrend")

    data['Parabolic SAR'] = sar
    data['Trend'] = trend
    return data

# 예제 데이터
if __name__ == "__main__":
    data = {
        'Date': pd.date_range(start="2023-01-01", periods=10),
        'High': [110, 115, 112, 118, 120, 119, 125, 130, 128, 132],
        'Low': [100, 105, 102, 108, 110, 109, 115, 120, 118, 122],
    }
    df = pd.DataFrame(data)

    # 파라볼릭 SAR 계산
    df = calculate_parabolic_sar(df)
    print(df)





def calculate_trix(data, period: int = 15):
    """
    TRIX (Triple Exponential Moving Average) 계산
    :param data: pandas DataFrame, 'Close' 컬럼 필수
    :param period: TRIX 계산에 사용할 기간 (기본값: 15)
    :return: TRIX가 추가된 DataFrame
    """
    # 1단계: 3중 지수 이동평균 계산
    ema1 = data['Close'].ewm(span=period, adjust=False).mean()
    ema2 = ema1.ewm(span=period, adjust=False).mean()
    ema3 = ema2.ewm(span=period, adjust=False).mean()

    # 2단계: TRIX 계산
    trix = (ema3 - ema3.shift(1)) / ema3.shift(1) * 100

    # 데이터프레임에 TRIX 추가
    data['TRIX'] = trix
    return data

# 예제 데이터
if __name__ == "__main__":
    data = {
        'Date': pd.date_range(start="2023-01-01", periods=20),
        'Close': [10, 10.5, 11, 10.8, 11.2, 11.5, 11.7, 12, 12.2, 12.5,
                  12.8, 13, 13.5, 13.8, 14, 14.2, 14.5, 15, 15.2, 15.5],
    }
    df = pd.DataFrame(data)

    # TRIX 계산
    df = calculate_trix(df)
    print(df)






def calculate_vortex(data, period: int = 14):
    """
    Vortex Indicator (VI) 계산
    :param data: pandas DataFrame, 'High', 'Low', 'Close' 컬럼 필수
    :param period: VI 계산에 사용할 기간 (기본값: 14)
    :return: VI+와 VI-가 추가된 DataFrame
    """
    # 1. VM+와 VM- 계산
    data['VM+'] = abs(data['High'] - data['Low'].shift(1))
    data['VM-'] = abs(data['Low'] - data['High'].shift(1))
    
    # 2. TR 계산
    data['TR'] = data[['High', 'Low', 'Close']].apply(
        lambda x: max(x['High'] - x['Low'], 
                      abs(x['High'] - x['Close'].shift(1)), 
                      abs(x['Low'] - x['Close'].shift(1))),
        axis=1
    )
    
    # 3. 이동합계(SUM) 계산
    data[f'SUM_VM+'] = data['VM+'].rolling(window=period).sum()
    data[f'SUM_VM-'] = data['VM-'].rolling(window=period).sum()
    data[f'SUM_TR'] = data['TR'].rolling(window=period).sum()

    # 4. VI+와 VI- 계산
    data['VI+'] = data[f'SUM_VM+'] / data[f'SUM_TR']
    data['VI-'] = data[f'SUM_VM-'] / data[f'SUM_TR']

    # 필요 없는 중간 계산 컬럼 제거
    data = data.drop(columns=['VM+', 'VM-', 'TR', f'SUM_VM+', f'SUM_VM-', f'SUM_TR'])
    
    return data

# 예제 데이터
if __name__ == "__main__":
    data = {
        'Date': pd.date_range(start="2023-01-01", periods=20),
        'High': [10, 11, 12, 11.5, 12.5, 13, 13.5, 13.8, 14, 14.5, 15, 15.2, 15.5, 15.8, 16, 16.5, 16.8, 17, 17.5, 18],
        'Low': [9, 9.5, 10, 10.2, 11, 11.5, 12, 12.5, 13, 13.2, 13.5, 14, 14.2, 14.5, 15, 15.2, 15.5, 15.8, 16, 16.5],
        'Close': [9.5, 10.5, 11, 11.3, 12, 12.8, 13, 13.2, 13.8, 14.2, 14.8, 15, 15.3, 15.5, 15.8, 16.3, 16.5, 16.8, 17, 17.8],
    }
    df = pd.DataFrame(data)

    # Vortex Indicator 계산
    df = calculate_vortex(df)
    print(df)
'''