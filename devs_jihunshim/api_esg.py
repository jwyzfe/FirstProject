from commons.api_send_requester import ApiRequester

class api_test_class:
    def api_test_func(test):
        # api
        city_list = ['도쿄','괌','모나코']
        key_list = ['lat', 'lon']
        pub_key = '39fb7b1c6d4e11e7483aabcb737ce7b0'
        result_cont = []
        for city in city_list:
            base_url = f'https://api.openweathermap.org/geo/1.0/direct'
            
            params={}
            params['q'] = city
            params['appid'] = pub_key

            result_geo = ApiRequester.send_api(base_url, params, key_list)

            base_url = f'https://pro.openweathermap.org/data/2.5/weather'
            
            params_w = {}
            for geo in result_geo:
                for key in key_list:
                    params_w[key] = geo[key]
            params_w['appid'] = pub_key
            # result_cont = ApiRequester.send_api(base_url, params_w)
            result_cont.append(ApiRequester.send_api(base_url, params_w))

            # print(result_cont)
        return result_cont

def run():

    api_test_class.api_test_func()
    
    pass



if __name__ == '__main__':
    run()
    pass
