
# 공통 부분을 import 하여 구현
from commons.api_send_requester import ApiRequester

class api_test_class:
    def api_test_func():

        # api
        city_list = ['tokyo']
        key_list = ['lat', 'lon']
        pub_key = '94e06b4441ef9588585af065e2b72b40'
        result_cont = []
        for city in city_list:
            base_url = f'https://api.openweathermap.org/geo/1.0/direct'
            
            params={}
            params['bizYear'] = bizYear
            params['crno'] = 

            result_geo = ApiRequester.send_api(base_url, params, key_list)

            base_url = f'https://api.openweathermap.org/data/2.5/forecast'
            
            params_w = {}
            for geo in result_geo:
                for key in key_list:
                    params_w[key] = geo[key]
            params_w['appid'] = pub_key
            # result_cont = ApiRequester.send_api(base_url, params_w)
            result_cont.append(ApiRequester.send_api(base_url, params_w))

            # print(result_cont)
        return result_cont

if __name__ == "__main__":
    api_test_class.api_test_func()
    pass