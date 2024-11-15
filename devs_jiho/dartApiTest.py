import requests
import io
import zipfile
import xml.etree.ElementTree as ET
import xmltodict
import json
from tqdm import tqdm
import logging
import time
from datetime import datetime
import os
import glob

class DartFinancialService:
    def __init__(self):
        # 로깅 설정
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.logger = logging.getLogger(__name__)
        self.BATCH_SIZE = 10000
        self.OUTPUT_DIR = 'data/dart'
        os.makedirs(self.OUTPUT_DIR, exist_ok=True)

    def get_corp_code(self):
        self.logger.info("DART API에서 기업 코드 데이터 요청 중...")
        
        DART_API_URL = "https://opendart.fss.or.kr/api/corpCode.xml"
        DART_API_KEY = "19307cd5ad804dd73f156678b8b4cffa1d83122e"
        
        params = {'crtfc_key': DART_API_KEY}
        response = requests.get(DART_API_URL, params=params)
        
        if response.status_code == 200:
            self.logger.info("기업 코드 데이터 성공적으로 수신")
            return response.content
        else:
            self.logger.error(f"DART API 오류: {response.status_code}")
            self.logger.error(response.text)
            return None

    def get_company_registration_number(self, corp_code):
        COMPANY_API_URL = "https://opendart.fss.or.kr/api/company.json"
        DART_API_KEY = "19307cd5ad804dd73f156678b8b4cffa1d83122e"
        
        params = {
            'crtfc_key': DART_API_KEY,
            'corp_code': corp_code
        }
        
        response = requests.get(COMPANY_API_URL, params=params)
        
        if response.status_code == 200:
            company_info = json.loads(response.text)
            corp_cls = company_info.get('corp_cls')
            if corp_cls == 'Y':
                return company_info
            return None
        return None

    def get_financial_statements(self, registration_number):
        financial_statements = []
        FINANCIAL_API_URL = "http://apis.data.go.kr/1160100/service/GetFinaStatInfoService_V2/getBs_V2"
        FINANCIAL_API_KEY = "stCwIy%2BLoSZpTsxJ4usyyYV3yWNUePAIHISVLlDaFUMfDQDW77n8maQ0pmOH4aJy25qsGso7nLjNXokXaJDGlQ%3D%3D"
        
        self.logger.info(f"기업 번호 {registration_number}의 재무제표 조회 중...")
        
        for year in tqdm(range(2017, 2023), desc="연도별 데이터 수집"):
            params = {
                'numOfRows': '1',
                'pageNo': '1',
                'resultType': 'json',
                'serviceKey': FINANCIAL_API_KEY,
                'crno': registration_number,
                'bizYear': str(year)
            }
            
            response = requests.get(FINANCIAL_API_URL, params=params)
            
            if response.status_code == 200:
                financial_data = json.loads(response.text)
                financial_statements.append(financial_data)
            else:
                self.logger.warning(f"{year}년도 데이터 조회 실패")

        return financial_statements

    def save_batch_results(self, registration_numbers, batch_num):
        """배치 결과를 JSON 파일로 저장"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{self.OUTPUT_DIR}/registration_numbers_batch_{batch_num}_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(registration_numbers, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"배치 {batch_num} 결과가 {filename}에 저장되었습니다.")

    def process_company_data(self):
        registration_numbers = []
        corp_codes = self.get_corp_code()
        batch_num = 1

        if corp_codes:
            with io.BytesIO(corp_codes) as zip_stream:
                with zipfile.ZipFile(zip_stream) as zip_file:
                    for file_name in zip_file.namelist():
                        with zip_file.open(file_name) as xml_file:
                            companies_data = xmltodict.parse(xml_file.read())
                            companies_list = companies_data['result']['list']
                            total_companies = len(companies_list)
                            
                            self.logger.info(f"총 {total_companies}개 기업 처리 시작")
                            
                            # 기업 리스트를 BATCH_SIZE 크기로 분할
                            for i in range(0, total_companies, self.BATCH_SIZE):
                                batch_companies = companies_list[i:i + self.BATCH_SIZE]
                                batch_registration_numbers = []
                                
                                self.logger.info(f"배치 {batch_num} 처리 시작 ({i+1}~{min(i+self.BATCH_SIZE, total_companies)} 기업)")
                                
                                for company in tqdm(batch_companies, 
                                                  desc=f"배치 {batch_num} 처리중",
                                                  position=0):
                                    corp_code = company['corp_code']
                                    
                                    try:
                                        reg_number = self.get_company_registration_number(corp_code)
                                        if reg_number:
                                            batch_registration_numbers.append({
                                                'corp_code': corp_code,
                                                'registration_number': reg_number
                                            })
                                    except Exception as e:
                                        self.logger.error(f"기업 코드 {corp_code} 처리 중 오류 발생: {str(e)}")
                                    
                                    # API 호출 제한을 위한 딜레이
                                    time.sleep(0.1)  # 100ms 딜레이
                                
                                # 배치 결과 저장 후 바로 재무제표 처리
                                self.save_batch_results(batch_registration_numbers, batch_num)
                                self.process_financial_statements_for_batch(batch_registration_numbers)
                                registration_numbers.extend(batch_registration_numbers)
                                
                                self.logger.info(f"배치 {batch_num} 완료. {len(batch_registration_numbers)}개 기업 처리됨")
                                batch_num += 1
                                
                                # 배치 간 딜레이
                                if i + self.BATCH_SIZE < total_companies:
                                    self.logger.info("다음 배치 처리를 위해 1분 대기")
                                    time.sleep(60)  # 1분 대기

        self.logger.info(f"전체 데이터 처리 완료. 총 {len(registration_numbers)}개 기업의 등록번호 수집")
        return registration_numbers

    def process_financial_statements_for_batch(self, companies):
        """단일 배치의 기업들에 대한 재무제표 데이터를 수집하고 저장"""
        financial_dir = f"{self.OUTPUT_DIR}/financial_statements"
        os.makedirs(financial_dir, exist_ok=True)
        
        self.logger.info(f"배치 재무제표 처리 시작 (총 {len(companies)}개 기업)")
        
        for company in tqdm(companies, desc="기업 재무제표 수집"):
            reg_number = company['registration_number']
            corp_code = company['corp_code']
            
            # 이미 처리된 파일 확인
            output_file = f"{financial_dir}/{corp_code}_financial_statements.json"
            if os.path.exists(output_file):
                self.logger.info(f"기업 코드 {corp_code}는 이미 처리되었습니다. 건너뜁니다.")
                continue
            
            try:
                # 재무제표 데이터 수집
                financial_data = self.get_financial_statements(reg_number)
                
                # 결과 저장
                if financial_data:
                    result = {
                        'corp_code': corp_code,
                        'registration_number': reg_number,
                        'financial_statements': financial_data
                    }
                    
                    with open(output_file, 'w', encoding='utf-8') as f:
                        json.dump(result, f, ensure_ascii=False, indent=2)
                    
                    self.logger.info(f"기업 코드 {corp_code}의 재무제표가 저장되었습니다.")
                
                # API 호출 제한을 위한 딜레이
                time.sleep(0.5)
                
            except Exception as e:
                self.logger.error(f"기업 코드 {corp_code} 처리 중 오류 발생: {str(e)}")
                continue
        
        self.logger.info("배치 재무제표 처리 완료")

    def resume_from_saved_data(self, last_batch_num):
        """저장된 데이터부터 이어서 처리"""
        saved_data = []
        for batch in range(1, last_batch_num):
            try:
                # 가장 최근 배치 파일 찾기
                batch_files = glob.glob(f"{self.OUTPUT_DIR}/registration_numbers_batch_{batch}_*.json")
                if batch_files:
                    latest_file = max(batch_files, key=os.path.getctime)
                    with open(latest_file, 'r', encoding='utf-8') as f:
                        batch_data = json.load(f)
                        saved_data.extend(batch_data)
                    self.logger.info(f"배치 {batch} 데이터 로드 완료: {latest_file}")
            except Exception as e:
                self.logger.error(f"배치 {batch} 데이터 로드 중 오류 발생: {str(e)}")
        
        return saved_data

    def get_financial_statements_from_files(self):
        """data 폴더의 registration 파일들에서 기업 정보를 읽어와 재무제표를 조회합니다."""
        financial_dir = f"{self.OUTPUT_DIR}/financial_statements"
        os.makedirs(financial_dir, exist_ok=True)
        
        # registration으로 시작하는 모든 파일 찾기
        registration_files = glob.glob(f"{self.OUTPUT_DIR}/registration_numbers_batch_*.json")
        
        self.logger.info(f"총 {len(registration_files)}개의 registration 파일을 찾았습니다.")
        
        for file_path in tqdm(registration_files, desc="파일 처리 중"):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    companies = json.load(f)
                    
                self.logger.info(f"{file_path} 파일에서 {len(companies)}개 기업 데이터를 로드했습니다.")
                
                # 각 기업의 재무제표 조회
                for company in tqdm(companies, desc="기업 재무제표 수집"):
                    reg_number = company['registration_number']
                    corp_code = company['corp_code']
                    
                    # 이미 처리된 파일 확인
                    output_file = f"{financial_dir}/{corp_code}_financial_statements.json"
                    if os.path.exists(output_file):
                        self.logger.info(f"기업 코드 {corp_code}는 이미 처리되었습니다. 건너뜁니다.")
                        continue
                    
                    # 재무제표 데이터 수집
                    financial_data = self.get_financial_statements(reg_number)
                    
                    # 결과 저장
                    if financial_data:
                        result = {
                            'corp_code': corp_code,
                            'registration_number': reg_number,
                            'financial_statements': financial_data
                        }
                        
                        with open(output_file, 'w', encoding='utf-8') as f:
                            json.dump(result, f, ensure_ascii=False, indent=2)
                        
                        self.logger.info(f"기업 코드 {corp_code}의 재무제표가 저장되었습니다.")
                    
                    # API 호출 제한을 위한 딜레이
                    time.sleep(0.5)
                    
            except Exception as e:
                self.logger.error(f"{file_path} 처리 중 오류 발생: {str(e)}")
                continue

    def process_batch(self, batch_number):
        """특정 배치 번호의 데이터만 처리"""
        self.logger.info(f"배치 {batch_number} 처리 시작")
        
        # 기존 데이터 파일 찾기
        registration_files = glob.glob(f"{self.OUTPUT_DIR}/registration_numbers_batch_{batch_number}_*.json")
        
        if not registration_files:
            self.logger.error(f"배치 {batch_number}의 registration 파일을 찾을 수 없습니다.")
            return
        
        # 가장 최근 파일 사용
        latest_file = max(registration_files, key=os.path.getctime)
        
        try:
            with open(latest_file, 'r', encoding='utf-8') as f:
                companies = json.load(f)
            
            self.logger.info(f"배치 {batch_number}에서 {len(companies)}개 기업 데이터를 로드했습니다.")
            
            financial_dir = f"{self.OUTPUT_DIR}/financial_statements/batch_{batch_number}"
            os.makedirs(financial_dir, exist_ok=True)
            
            # 각 기업의 재무제표 조회
            for company in tqdm(companies, desc=f"배치 {batch_number} 재무제표 수집"):
                reg_number = company['registration_number']
                corp_code = company['corp_code']
                
                # 이미 처리된 파일 확인
                output_file = f"{financial_dir}/{corp_code}_financial_statements.json"
                if os.path.exists(output_file):
                    self.logger.info(f"기업 코드 {corp_code}는 이미 처리되었습니다. 건너뜁니다.")
                    continue
                
                try:
                    # 재무제표 데이터 수집
                    financial_data = self.get_financial_statements(reg_number)
                    
                    # 결과 저장
                    if financial_data:
                        result = {
                            'corp_code': corp_code,
                            'registration_number': reg_number,
                            'financial_statements': financial_data
                        }
                        
                        with open(output_file, 'w', encoding='utf-8') as f:
                            json.dump(result, f, ensure_ascii=False, indent=2)
                        
                        self.logger.info(f"기업 코드 {corp_code}의 재무제표가 저장되었습니다.")
                    
                    # API 호출 제한을 위한 딜레이
                    time.sleep(0.5)
                    
                except Exception as e:
                    self.logger.error(f"기업 코드 {corp_code} 처리 중 오류 발생: {str(e)}")
                    continue
                
        except Exception as e:
            self.logger.error(f"배치 {batch_number} 처리 중 오류 발생: {str(e)}")

if __name__ == "__main__":
    dart_service = DartFinancialService()
    
    # 처리할 배치 번호 지정
    batch_number = 1  # 원하는 배치 번호로 변경
    dart_service.process_batch(batch_number)
