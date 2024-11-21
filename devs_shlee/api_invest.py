
import investpy
import pandas
class api_call_invest:
    def api_test_func(test):
        search_result = investpy.search_quotes(text='apple', products=['stocks'],
                                            countries=['united states'], n_results=1)
        print(search_result)

if __name__ == "__main__":
    api_call_invest.api_test_func(1)
