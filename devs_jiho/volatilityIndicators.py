# numpy 라이브러리 임포트 - 수학적 계산(제곱근 등)에 필요
import numpy as np

# [1] 평균진정 가격범위 (Average True Range), EMA 적용 NOT SMA
# 단순 이동평균(SMA) - 모든 기간에 동일한 가중치 ; EMA - Wilder의 평활화 방법 사용 최근 데이터에 더 높은 가중치
# https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/atr


def calculate_ATR(df, period=14):
    """
    ATR(Average True Range) 계산 함수
    - 변동성을 측정하는 지표로, 가격의 평균적인 변동 범위를 보여줌
    - period=14는 일반적으로 사용되는 표준 기간
    """
    # TR1: 당일 고가와 저가의 차이 (당일 가격 변동폭)
    df['TR1'] = df['HIGH'] - df['LOW']
    
    # TR2: 당일 고가와 전일 종가의 차이 (갭 상승 고려)
    df['TR2'] = abs(df['HIGH'] - df['CLOSE'].shift(1))
    
    # TR3: 당일 저가와 전일 종가의 차이 (갭 하락 고려)
    df['TR3'] = abs(df['LOW'] - df['CLOSE'].shift(1))
    
    # TR: 세 가지 범위 중 가장 큰 값을 선택 (실제 가격 변동의 최대 범위)
    df['TR'] = df[['TR1', 'TR2', 'TR3']].max(axis=1)
    
    # ATR 초기값 설정 (첫 번째 TR 값으로 시작)
    df['ATR'] = df['TR'].copy()
    
    # Wilder의 평활화 방법으로 ATR 계산
    # - 이전 ATR에 더 큰 가중치를 두어 급격한 변화를 완화
    for i in range(1, len(df)):
        df.loc[df.index[i], 'ATR'] = (df.loc[df.index[i-1], 'ATR'] * (period-1) + 
                                     df.loc[df.index[i], 'TR']) / period
    
    # 최종 ATR 값 저장
    ATR = df['ATR']
    
    # 계산에 사용된 임시 컬럼들 제거 (메모리 효율성)
    df.drop(['TR1', 'TR2', 'TR3', 'TR', 'ATR'], axis=1, inplace=True)
    
    return ATR

# [2] 돈치안 채널 (Donchian channel)
# https://primestory.tistory.com/19
# https://github.com/Mayankraj23/Donchian-Channel-TI/blob/master/donchian_channel.py


def calculate_donchian_channel(df, period=20):
    """
    돈치안 채널 계산 함수
    - 가격의 상하한 범위를 보여주는 지표
    - period=20은 일반적으로 사용되는 기간
    """
    # 상단밴드: 설정 기간 동안의 최고가
    # rolling().max()로 이동 최대값 계산
    upper_band = df['HIGH'].rolling(window=period).max()
    
    # 하단밴드: 설정 기간 동안의 최저가
    # rolling().min()으로 이동 최소값 계산
    lower_band = df['LOW'].rolling(window=period).min()
    
    # 중간밴드: 상단과 하단의 중간값
    # 현재 추세의 중심선으로 사용
    middle_band = (upper_band + lower_band) / 2
    
    return upper_band, lower_band, middle_band

# [3] 켈트너 채널 (Keltner channel)
# https://primestory.tistory.com/17
"""
    켈트너 채널(Keltner Channel) 계산 함수
    
    켈트너 채널 트레이딩 전략:
    1. 상단 돌파(매수 신호)
       - 가격이 상단밴드를 상향 돌파할 때 강세 신호
       - 추가 상승 모멘텀 예상
    
    2. 하단 돌파(매도 신호)
       - 가격이 하단밴드를 하향 돌파할 때 약세 신호
       - 추가 하락 모멘텀 예상
    
    3. 중앙선 활용
       - 중기 추세의 방향성 판단
       - 가격이 중앙선 위에 있으면 상승추세, 아래에 있으면 하락추세
"""

def calculate_keltner_channel(df, ema_period=20, atr_period=14, multiplier=2.0):
    """
    켈트너 채널 계산 함수
    - 볼린저 밴드와 비슷하나, 표준편차 대신 ATR을 사용
    - EMA와 ATR을 기반으로 하여 가격 변동성을 더 부드럽게 반영
    """
    # 중앙선 계산: 종가의 지수이동평균(EMA)
    # - adjust=False: 데이터 시작부분의 편향을 조정하지 않음
    # - span=ema_period: 지수적으로 감소하는 가중치의 기간
    middle_line = df['CLOSE'].ewm(span=ema_period, adjust=False).mean()
    
    # ATR 계산: 가격의 평균 변동폭
    # - 별도로 정의된 calculate_ATR 함수 사용
    atr = calculate_ATR(df, period=atr_period)
    
    # 상단 밴드: 중앙선 + (ATR * multiplier)
    # - multiplier로 변동성 범위 조절 (기본값 2.0)
    upper_band = middle_line + (multiplier * atr)
    
    # 하단 밴드: 중앙선 - (ATR * multiplier)
    # - 상단 밴드와 대칭되는 하향 범위
    lower_band = middle_line - (multiplier * atr)
    
    return upper_band, middle_line, lower_band


# [4] 변동성지수 (VIX)

"""
    개별 주식의 역사적 변동성 계산 함수
    
    Parameters:
    df: DataFrame - 'close' 컬럼이 포함된 데이터프레임
    period: int - 변동성 계산 기간 (기본값 20일)
    
    Returns:
    Series - 연율화된 변동성 (%)
"""

def calculate_historical_volatility(df, period=20):
    """
    역사적 변동성 계산 함수
    - 주가의 변동성을 연간화된 백분율로 표시
    - period=20은 한 달 정도의 거래일 수
    """
    # 일간 수익률 계산 (당일 종가 / 전일 종가 - 1)
    daily_returns = df['CLOSE'].pct_change()
    
    # 변동성 계산:
    # 1. rolling().std(): 일간 수익률의 표준편차
    # 2. np.sqrt(252): 연간화 팩터 (252는 연간 거래일 수)
    # 3. * 100: 백분율 변환
    volatility = daily_returns.rolling(window=period).std() * np.sqrt(252) * 100
    
    return volatility

# [5] 표준 편차 (σ)
# https://www.interactivebrokers.com/campus/ibkr-quant-news/standard-deviation-in-trading-calculations-use-cases-examples-and-more-part-i/

"""
    주가 데이터의 표준편차 계산 함수
    
    Parameters:
    df: DataFrame - 'open', 'high', 'low', 'close', 'volume' 컬럼이 포함된 데이터프레임
    period: int - 표준편차 계산 기간 (기본값 20일)
    column: str - 계산에 사용할 컬럼 (기본값 'close')
    
    Returns:
    Series - 표준편차 값
"""

def calculate_standard_deviation(df, period=20, column='CLOSE'):

    # 일간 변동률 계산
    # - pct_change(): 전일 대비 변화율
    daily_returns = df[column].pct_change()
    
    # 표준편차 계산
    # - rolling(window=period): 설정된 기간동안의 이동창
    # - std(): 표준편차 계산
    std_dev = daily_returns.rolling(window=period).std()
    
    return std_dev

# 표준편차 각 칼럼별 계산 과연 필요할까?

'''
def calculate_price_std(df, period=20):
    """
    OHLC 가격 전체의 표준편차 계산 함수
    - 시가, 고가, 저가, 종가 각각의 변동성을 동시에 분석
    - 가격의 전반적인 변동성 패턴을 파악하는데 유용
    """
    # 분석할 가격 컬럼들 정의
    # - OHLC(시가,고가,저가,종가) 전체를 대상으로 함
    price_columns = ['open', 'high', 'low', 'close']
    
    # 결과를 저장할 빈 데이터프레임 생성
    std_results = pd.DataFrame()
    
    # 각 가격 컬럼별로 표준편차 계산
    # - f'{col}_std': 각 컬럼명에 _std 접미사 추가
    for col in price_columns:
        std_results[f'{col}_std'] = calculate_standard_deviation(df, period, col)
    
    return std_results '''

# [6] 분산 (σ2) 관련 주석
"""
분산 계산을 이유:
1. 개별 주식에서는 표준편차가 더 직관적인 해석 제공
2. 분산은 표준편차의 제곱이므로, 필요시 표준편차 값을 제곱하여 사용 가능
"""

