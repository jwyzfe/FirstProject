import json
import requests
import pandas as pd
from datetime import datetime


class CompanyFinancials:

    def get_financial_statements(regist_number_list):
        all_company_data = []
        
        FINANCIAL_API_URL = "http://apis.data.go.kr/1160100/service/GetFinaStatInfoService_V2/getBs_V2"
        FINANCIAL_API_KEY = "stCwIy+LoSZpTsxJ4usyyYV3yWNUePAIHISVLlDaFUMfDQDW77n8maQ0pmOH4aJy25qsGso7nLjNXokXaJDGlQ=="

        for registration_number in regist_number_list:
            if not registration_number:
                continue
                
            registration_number = registration_number.replace("-", "")
            
            try:
                params = {
                    'serviceKey': FINANCIAL_API_KEY,
                    'crno': registration_number,
                    'resultType': 'json',
                    'pageNo': '1',
                    'numOfRows': '1'
                }
                
                response = requests.get(FINANCIAL_API_URL, params=params)
                response.raise_for_status()
                
                data = response.json()
                
                if 'response' in data and 'body' in data['response'] and 'items' in data['response']['body']:
                    company_data = data['response']['body']['items']
                    
                    if company_data:
                        # items 데이터 추출
                        if isinstance(company_data, dict) and 'item' in company_data:
                            items = company_data['item']
                            if not isinstance(items, list):
                                items = [items]
                        elif isinstance(company_data, list):
                            items = company_data
                        else:
                            continue
                            
                        # 각 항목을 DataFrame으로 변환
                        rows = []
                        for item in items:
                            if isinstance(item, dict):
                                row = {
                                    'base_date': item.get('basDt'),
                                    'corp_code': item.get('crno'),
                                    'business_year': item.get('bizYear'),
                                    'financial_statement_code': item.get('fnclDcd'),
                                    'financial_statement_name': item.get('fnclDcdNm'),
                                    'account_id': item.get('acitId'),
                                    'account_name': item.get('acitNm'),
                                    'current_quarter_amount': item.get('thqrAcitAmt'),
                                    'current_term_amount': item.get('crtmAcitAmt'),
                                    'previous_quarter_amount': item.get('lsqtAcitAmt'),
                                    'previous_term_amount': item.get('pvtrAcitAmt'),
                                    'before_previous_term_amount': item.get('bpvtrAcitAmt'),
                                    'currency_code': item.get('curCd')
                                }
                                rows.append(row)
                            
                        if rows:  # rows가 비어있지 않은 경우에만 DataFrame 생성
                            df = pd.DataFrame(rows)
                            
                            # 금액 컬럼 숫자 변환
                            amount_columns = [
                                'current_quarter_amount',
                                'current_term_amount',
                                'previous_quarter_amount',
                                'previous_term_amount',
                                'before_previous_term_amount'
                            ]
                            
                            for col in amount_columns:
                                if col in df.columns:
                                    df[col] = pd.to_numeric(df[col], errors='coerce')
                            
                            # 날짜 변환
                            if 'base_date' in df.columns:
                                df['base_date'] = pd.to_datetime(df['base_date'], format='%Y%m%d')
                            
                            all_company_data.append(df)
                            # print(f"Processed corp_code: {registration_number}")
                        else :
                            print(f'empty row. corp_code {registration_number}')
                    
            except Exception as e:
                print(f"Error processing corp_code {registration_number}: {str(e)}")
                continue
        
        if all_company_data:
            try:
                final_df = pd.concat(all_company_data, ignore_index=True)
                sort_columns = ['corp_code', 'base_date', 'account_name']
                final_df = final_df.sort_values(sort_columns)
                return final_df
            except Exception as e:
                print(f"Error concatenating DataFrames: {str(e)}")
                return pd.DataFrame()
        
        return pd.DataFrame()
        

# 실행 코드
if __name__ == "__main__":

    financial_data = CompanyFinancials.get_financial_statements()
    