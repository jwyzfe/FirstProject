
import pandas as pd 
import numpy as np


class IndicatorElasticityFormulas:

    '''
    파라미터 
    특정 symbol의 특정 기간 동안의 high low close volume 
    n 이 특정 되야 하는 경우 어떻게? 
    case 
    - total data 100
    - bollinger band 14
    - 86일 치 


    '''

    @classmethod
    def execute_formula_func(cls, df_price, n):
        # 각 함수가 필요로 하는 일수 만큼 잘라서 넣기
        # 결과를 저장할 빈 DataFrame 생성
        df_totals = pd.DataFrame(columns=['Upper Band', 'Mid Band', 'Lower Band'])

        # df_price의 길이에서 n을 뺀 만큼 반복
        for i in range(n - 1, len(df_price)):
            # n일치의 Close 데이터 슬라이싱
            batch = df_price.iloc[i-n+1:i+1]
            # 볼린저 밴드 계산
            df_result = cls.bollinger_band(batch)
            # 볼린저 밴드 계산
            df_result = cls.bollinger_band(batch)
            # 결과를 DataFrame에 추가
            df_totals = pd.concat([df_totals, df_result], ignore_index=True)
        return df_totals
    
    @classmethod
    def bollinger_band(cls, df_price, k=2): # 무조건 length 만큼 
        mid_band = cls.simple_moving_average(df_price)
        upper_band = cls.simple_moving_average(df_price) + (k*np.std(df_price["Close"]))
        lower_band = cls.simple_moving_average(df_price) - (k*np.std(df_price["Close"]))
        
        # 결과를 DataFrame으로 반환
        df_result = pd.DataFrame({
            'Upper Band': [upper_band],
            'Mid Band': [mid_band],
            'Lower Band': [lower_band]
        })

        return df_result


    @classmethod
    def stochastic(cls):
        pass

    @classmethod
    def relative_volatility_index(cls):
        pass

    @classmethod
    def elder_ray_power(cls):
        pass

    @classmethod
    def average_true_range(cls):
        pass

    @classmethod
    def bands_width(cls):
        pass

    @classmethod
    def accumulation_swing_index(cls):
        pass

    @classmethod
    def rsmacd(cls):
        pass

    @classmethod
    def stochastic_rsi(cls):
        pass

    @classmethod
    def rmi(cls):
        pass

    @classmethod
    def simple_moving_average(cls, df_prices):
        return np.mean(df_prices["Close"])


    @classmethod
    def exponential_moving_average(cls):
        pass



def run():
    n = 21
    # 예시 데이터 생성
    data = {
        'Open': np.random.rand(100) * 100,
        'Low': np.random.rand(100) * 100,
        'High': np.random.rand(100) * 100,
        'Close': np.random.rand(100) * 100,
        'Volume': np.random.randint(1, 1000, size=100)
    }

    df_price = pd.DataFrame(data)

    # 볼린저 밴드 계산
    result_df = IndicatorElasticityFormulas.execute_formula_func(df_price, n=20)

    pass


if __name__ == "__main__" :
    
    run()
    pass