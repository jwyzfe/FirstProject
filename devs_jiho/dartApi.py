import json
import requests
import pandas as pd
from datetime import datetime
import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

class CompanyFinancials:
    def __init__(self):
        # 로깅 설정
        self.logger = self._setup_logger()
        
        # API 설정
        self.FINANCIAL_API_URL = "http://apis.data.go.kr/1160100/service/GetFinaStatInfoService_V2/getBs_V2"
        self.FINANCIAL_API_KEY = "stCwIy+LoSZpTsxJ4usyyYV3yWNUePAIHISVLlDaFUMfDQDW77n8maQ0pmOH4aJy25qsGso7nLjNXokXaJDGlQ=="
        
        # 회사 정보 딕셔너리
        self.dictCor ####### companyList.json 파일 참조 부탁드립니다 :)

    def _setup_logger(self):
        logger = logging.getLogger('CompanyFinancials')
        logger.setLevel(logging.INFO)
        
        # 파일 핸들러 설정
        fh = logging.FileHandler('financial_data.log')
        fh.setLevel(logging.INFO)
        
        # 콘솔 핸들러 설정
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        
        # 포맷터 설정
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        logger.addHandler(fh)
        logger.addHandler(ch)
        
        return logger

    def get_financial_statements(self):
        financial_statements = []
        
        # 결과를 저장할 디렉토리 생성
        output_dir = 'financial_data'
        os.makedirs(output_dir, exist_ok=True)
        
        for company_name, registration_number in self.dictCor[0].items():
            if not registration_number:
                self.logger.warning(f"회사명 {company_name}에 대한 법인번호가 없습니다.")
                continue
                
            # 하이픈 제거
            registration_number = registration_number.replace("-", "")
            
            self.logger.info(f"기업 {company_name}(법인번호: {registration_number})의 재무제표 조회 중...")
            
            try:
                params = {
                    'serviceKey': self.FINANCIAL_API_KEY,
                    'crno': registration_number,
                    'resultType': 'json',
                    'pageNo': '1',
                    'numOfRows': '100'
                }
                
                response = requests.get(self.FINANCIAL_API_URL, params=params)
                response.raise_for_status()  # HTTP 오류 체크
                
                data = response.json()
                
                # API 응답 구조에 따라 데이터 추출
                if 'response' in data and 'body' in data['response'] and 'items' in data['response']['body']:
                    company_data = data['response']['body']['items']
                    
                    # 데이터프레임으로 변환
                    if company_data:
                        df = pd.DataFrame(company_data)
                        
                        # CSV 파일로 저장
                        output_file = os.path.join(output_dir, f"{company_name}_financial_data.csv")
                        df.to_csv(output_file, index=False, encoding='utf-8-sig')
                        
                        self.logger.info(f"{company_name} 재무제표 데이터 저장 완료: {output_file}")
                        
                        financial_statements.append({
                            'company_name': company_name,
                            'data': company_data
                        })
                    else:
                        self.logger.warning(f"{company_name}의 재무제표 데이터가 없습니다.")
                
            except requests.exceptions.RequestException as e:
                self.logger.error(f"기업 {company_name} API 호출 중 오류 발생: {str(e)}")
                continue
            except Exception as e:
                self.logger.error(f"기업 {company_name} 데이터 처리 중 오류 발생: {str(e)}")
                continue
                
        return financial_statements

    def save_to_json(self, financial_statements):
        """재무제표 데이터를 JSON 파일로 저장"""
        try:
            output_file = 'financial_statements.json'
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(financial_statements, f, ensure_ascii=False, indent=2)
            self.logger.info(f"전체 재무제표 데이터 JSON 저장 완료: {output_file}")
        except Exception as e:
            self.logger.error(f"JSON 파일 저장 중 오류 발생: {str(e)}")

# 실행 코드
if __name__ == "__main__":
    company_financials = CompanyFinancials()
    financial_data = company_financials.get_financial_statements()
    company_financials.save_to_json(financial_data)