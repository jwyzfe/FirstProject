
import json
from commons.api_send_requester import ApiRequester

class api_test_class:
    def dart_func(companyCode):
        # api
        numOfRows_list = []
        pageNo_list = []
        service_key = f'stCwIy%2BLoSZpTsxJ4usyyYV3yWNUePAIHISVLlDaFUMfDQDW77n8maQ0pmOH4aJy25qsGso7nLjNXokXaJDGlQ%3D%3D'
        resultType = json
        crno = {companyCode}
        bizYear = {}
        for balance_sheet in ??:
            base_url = f'http://apis.data.go.kr/1160100/service/GetFinaStatInfoService_V2'
'
            
            params={}
            params['q'] = city
            params['appid'] = pub_key

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
    api_test_class.dart_func()
    pass