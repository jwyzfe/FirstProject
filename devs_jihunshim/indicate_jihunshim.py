import yfinance as yf
import pandas as pd
import time

# 주가 데이터 실시간 업데이트 (저가, 고가, 종가, 거래량)
'---------------------------------------------------------------------------'
# def get_stock_data(ticker):
#     # 실시간으로 주가데이터를 가져오는 함수
#     data = yf.download(ticker, period='1d', interval='1h') # 최근 1일 동안 1시간 간격데이터
#     latest = data.iloc[-1] # 가장 최신 데이터
#     print(f'종목 : {ticker}')
#     print(f"고점 : {latest['High']}, 저점 : {latest['Low']}, 종가 : {latest['Close']}, 거래량 : {latest['Volume']}")
#     return latest

# # 반복적으로 데이터 가져오기
# if __name__=="__main__":
#     ticker = "AAPL" #애플주식
#     while True:
#         try:
#             latest_dat = get_stock_data(ticker)
#             time.sleep(60) # 1분마다 데이터 업데이트
#         except Exception as e :
#             print(f"오류 발생: {e}")
#             break
# '----------------------------------------------------------------------------'

# 1.누적분포 (Accumulation Distribution, A/D)

#CLV 계산
def calculate_clv(close, low, high):
    # CLV (Closing Line Value): 하루 동안 종가가 고가와 저가 사이에서 어디쯤 위치하는지 나타냄
    # 계산식 CLV = {(종가 - 저가) - (고가 - 종가)} / (고가 - 저가)
    
    # 계산코드
    if high != low:
        clv = ((close - low) - (high - close)) / (high - low)
    else:
        clv = 0
    return clv


# A/D 계산
def calculate_ad(previous_ad, clv, volume):
    
    # A/D : 누적된 매수/매도 압력.
    # 계산식 A/D = 이전 A/D + ( CLV * 거래량 ) 
    return previous_ad + (clv * volume)
    
# 예시 데이터
# close, low, high, volume = 150, 145, 155, 10000
# previous_ad = 50000

# clv = calculate_clv(close, low, high)
# ad = calculate_ad(previous_ad, clv, volume)

# print(f"CLV : {clv}, A/D : {ad}")
'---------------------------------------------------------------------------'
# 2. Ease of Movement (EOM)

def calculate_eom(high, low, prev_high, prev_low, volume, scale=1000000):
    
    # Ease of Movement (EOM) 계산
    # 주가의 변화량 = {(당일고가 + 당일저가) / 2} - {(전일고가 + 전일저가) / 2}
    # Box Ratio = (거래량/Scale) / (당일고가 - 당일저가)
    # Ease of Movement = 주가의 변화량 / Box Ratio 의 N일 단순이동평균
    # Signal = Ease of Movement의 M일 단순이동평균

    price_movement = ((high + low) / 2) - ((prev_high + prev_low) / 2)
    box_ratio = (volume / scale) / (high - low) if high != low else 0
    eom = price_movement / box_ratio if box_ratio != 0 else 0
    return eom

# 예시 데이터
# high, low = 155,145
# prev_high, prev_low = 152, 146
# volume = 10000

# eom = calculate_eom(high, low, prev_high,prev_low, volume)
# print(f"EOM : {eom}")
'---------------------------------------------------------------------------'

# 3. NVI 네거티브 볼륨 지수(Negative Volume Index)

def calculate_nvi(prev_nvi, today_close, prev_close, today_volume, prev_volume):
    
    # NVI (Negative Volume Index) 계산
    # NVI(오늘) = NVI(어제) + (오늘의 종가 변동률 / 100 * NVI(어제))
    # 조건 : 오늘 거래량이 어제보다 적을 때만 계산
    
    if today_volume < prev_volume:
        price_change = ((today_close - prev_close) / prev_close) * 100
        nvi = prev_nvi + (price_change / 100 * prev_nvi)
    else:
        nvi = prev_nvi
    return nvi

# 예시 데이터
# prev_nvi = 1000
# today_close, prev_close = 150, 145
# today_volume, prev_volume = 8000, 10000

# nvi = calculate_nvi(prev_nvi, today_close, prev_close, today_volume, prev_volume)
# print(f"NVI : {nvi}")

'---------------------------------------------------------------------------'

# 4. OBV 온벨런스 볼륨(On-Balance Volume)

def calculate_obv(prev_obv, today_close, prev_close, volume):
    
    #OBV (On-Balance Volume) 계산
    #OBV(오늘) = OBV(어제) + 거래량, 오늘 종가 > 어제 종가
               #OBV(어제) - 거래량, 오늘 종가 < 어제 종가
               #OBV(어제), 오늘 종가 = 어제 종가
    
    if today_close > prev_close:
        return prev_obv + volume
    elif today_close < prev_close:
        return prev_obv - volume
    else:
        return prev_obv

# 예시 데이터
# prev_obv = 50000
# today_close, prev_close = 150, 145
# volume = 10000

# obv =calculate_obv(prev_obv, today_close, prev_close, volume)
# print(f"OBV: {obv}")

' ---------------------------------------------------------------------------'

# 5. 풋 / 콜 비율 PCR (Put-Call Ratio)

def calculate_pcr(put_volume, call_volume):
    
    # PCR (Put-Call Ratio) 계산
    # 공식 : 풋 옵션 거래량 / 콜 옵션 거래량

    if call_volume == 0: # 콜 옵션 거래량이 0인 경우
        return float('inf') # 무한대 반환 (정의되지 않음)
    return put_volume / call_volume

# 예시 데이터
# put_volume = 1200
# call_volume = 800

# pcr = calculate_pcr(put_volume, call_volume)
# print(f"PCR (Put-Call Ratio): {pcr:.7f}") # 비율 소수점 2째자리 까지 표기

'---------------------------------------------------------------------------'

# 6. 거래량 - 가격 추세 VPT (Volume Price Trend)

def calculate_vpt(prev_vpt, today_close, prev_close, volume):
    
    # VPT (Volume Price Trend) 계산
    # 공식: VPT(오늘) = VPT(어제) + {(오늘 종가 - 어제 종가) / 어제 종가 * 거래량}
    
    if prev_close == 0: #어제 종가가 0인 경우 방지
        return prev_vpt
    price_change_ratio = (today_close - prev_close) / prev_close
    vpt = prev_vpt + (price_change_ratio * volume)
    return vpt

# 예시 데이터
# prev_vpt =500000
# today_close, prev_close = 150, 145
# volume = 10000

# vpt = calculate_vpt(prev_vpt, today_close, prev_close, volume)
# print(f"VPT (Volume Price Trend): {vpt}")

'---------------------------------------------------------------------------'

# 7. 차이킨 오실레이터 (Chaikin Oscillator)

def calculate_chaikin(ad, short_ma, long_ma):
    
    # Chaikin Osclillator 계산
    # 공식 A/D의 단기 이동평균 - 장기 이동평균
    
    return short_ma - long_ma

# 예시 데이터
# ad_values = [100, 200, 300, 400, 500] # A/D 값들
# short_ma = sum(ad_values[-3:]) / 3 # 단기 이동평균 (3일)
# long_ma = sum(ad_values) / len(ad_values) # 장기 이동평균

# chaikin_osc = calculate_chaikin(ad_values[-1], short_ma, long_ma)
# print(f"Chaikin Oscillator : {chaikin_osc}")

'---------------------------------------------------------------------------'

# 8. 피보나치 되돌림 수준

def calculate_fibonacci_retracement(high, low, ratio):
    
    # 피보나치 되돌림 수준 계산
    # 공식 : 고점 - (고점 - 저점) * 비율
    
    return high - (high - low) * ratio

# 예시 데이터
# high, low = 155, 145
# fibonacci_ratios = [0, 0.236, 0.382,0.5, 0.618, 1] # 비율 0%, 23.6%, 38.2%, 50%, 61.8%, 100%

# for ratio in fibonacci_ratios:
#     retracement = calculate_fibonacci_retracement(high, low, ratio)
#     print(f"Fibonacci {ratio*100:.1f}% Level: {retracement:.2f}")

'---------------------------------------------------------------------------'

# 9. 목선 돌파가격 및 목표 상승/하락 가격

def calculate_neckline_price_for_uptrend(high1, high2, low_price):
    
    # 목선 돌파가격 및 목표 상승 가격 계산
    # 목선 돌파가격 : 두 고점의 평균 가격
    # 목표 상승 가격 : 목선 돌파가격 + (목선 돌파가격 - 바닥가격)

    neckline = (high1 + high2) / 2
    target_price = neckline + (neckline - low_price)
    return neckline, target_price

def calculate_neckline_price_for_downtrend(low1, low2, high_price):
    
    # 목선 돌파가격 및 목표 하락 가격 계산
    # 목선 돌파가격: 두 저점의 평균 가격
    # 목표 하락 가격: 목선 돌파가격 - (고점 가격 - 목선 돌파가격)
    
    neckline = (low1 + low2) / 2
    target_price = neckline -(high_price - neckline)
    return neckline, target_price

# 예시 데이터 (상승돌파)
# high1, high2, low_price = 150, 155, 140
# neckline_up, target_up = calculate_neckline_price_for_uptrend(high1, high2, low_price)
# print(f"목선 돌파가격(상승): {neckline_up:.2f}, 목표 상승 가격: {target_up:.2f}")

# 예시 데이터 (하락돌파)
# low1, low2, high_price = 145, 140, 160
# neckline_down, target_down = calculate_neckline_price_for_downtrend(low1, low2, high_price)
# print(f"목선 돌파가격(하락): {neckline_down:.2f}, 목표 하락 가격: {target_down:.2f}")

'---------------------------------------------------------------------------'

if __name__ == '__main__':
    
    #1. clv, ad 계산
    close, low, high, volume = 150, 145, 155, 10000
    previous_ad = 50000

    clv = calculate_clv(close, low, high)
    ad = calculate_ad(previous_ad, clv, volume)

    print(f"CLV : {clv}, A/D : {ad}")

    #2. Ease of Movement (EOM) 계산
    high, low = 155, 145
    prev_high, prev_low = 152, 146
    volume = 10000

    eom = calculate_eom(high, low, prev_high,prev_low, volume)
    print(f"EOM : {eom}")
    
    # 3. NVI 네거티브 볼륨 지수(Negative Volume Index)
    prev_nvi = 1000
    today_close, prev_close = 150, 145
    today_volume, prev_volume = 8000, 10000

    nvi = calculate_nvi(prev_nvi, today_close, prev_close, today_volume, prev_volume)
    print(f"NVI : {nvi}")
    
    # 4. OBV 온벨런스 볼륨(On-Balance Volume)
    prev_obv = 50000
    today_close, prev_close = 150, 145
    volume = 10000

    obv =calculate_obv(prev_obv, today_close, prev_close, volume)
    print(f"OBV: {obv}")  
    
    # 5. 풋 / 콜 비율 PCR (Put-Call Ratio)
    put_volume = 1200
    call_volume = 800

    pcr = calculate_pcr(put_volume, call_volume)
    print(f"PCR (Put-Call Ratio): {pcr:.7f}") # 비율 소수점 2째자리 까지 표기
    
    # 6. 거래량 - 가격 추세 VPT (Volume Price Trend)
    prev_vpt =500000
    today_close, prev_close = 150, 145
    volume = 10000

    vpt = calculate_vpt(prev_vpt, today_close, prev_close, volume)
    print(f"VPT (Volume Price Trend): {vpt}")
    
    # 7. 차이킨 오실레이터 (Chaikin Oscillator)
    ad_values = [100, 200, 300, 400, 500] # A/D 값들
    short_ma = sum(ad_values[-3:]) / 3 # 단기 이동평균 (3일)
    long_ma = sum(ad_values) / len(ad_values) # 장기 이동평균

    chaikin_osc = calculate_chaikin(ad_values[-1], short_ma, long_ma)
    print(f"Chaikin Oscillator : {chaikin_osc}")
    
    # 8. 피보나치 되돌림 수준
    high, low = 155, 145
    fibonacci_ratios = [0, 0.236, 0.382,0.5, 0.618, 1] # 비율 0%, 23.6%, 38.2%, 50%, 61.8%, 100%

    for ratio in fibonacci_ratios:
        retracement = calculate_fibonacci_retracement(high, low, ratio)
        print(f"Fibonacci {ratio*100:.1f}% Level: {retracement:.2f}")
    
    # 9. 목선 돌파가격 및 목표 상승/하락 가격
    high1, high2, low_price = 150, 155, 140
    neckline_up, target_up = calculate_neckline_price_for_uptrend(high1, high2, low_price)
    print(f"목선 돌파가격(상승): {neckline_up:.2f}, 목표 상승 가격: {target_up:.2f}")

    low1, low2, high_price = 145, 140, 160
    neckline_down, target_down = calculate_neckline_price_for_downtrend(low1, low2, high_price)
    print(f"목선 돌파가격(하락): {neckline_down:.2f}, 목표 하락 가격: {target_down:.2f}")
 
pass

# module