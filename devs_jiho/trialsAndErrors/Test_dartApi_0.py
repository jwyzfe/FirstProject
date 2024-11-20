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

class DartCorpCodeCollector:
    """DART API를 통해 기업 코드를 수집하고 저장하는 클래스"""
    
    def __init__(self):
        # API 호출 제한을 고려한 배치 크기 설정 (한 번에 처리할 기업 수)
        self.BATCH_SIZE = 10000
        # 데이터를 저장할 기본 디렉토리 경로
        self.OUTPUT_DIR = 'data/dart'
        # 로깅 설정 초기화
        self._setup_logging()
        # 출력 디렉토리가 없으면 생성
        os.makedirs(self.OUTPUT_DIR, exist_ok=True)

    def _setup_logging(self):
        """로깅 설정을 초기화하는 메소드"""
        logging.basicConfig(
            level=logging.INFO,  # 로그 레벨 설정 (INFO 이상의 로그만 출력)
            format='%(asctime)s - %(levelname)s - %(message)s',  # 로그 형식 설정
            datefmt='%Y-%m-%d %H:%M:%S'  # 날짜 형식 설정
        )
        self.logger = logging.getLogger(__name__)

    def collect_corp_codes(self):
        """
        DART API에서 전체 기업 코드를 수집하고 배치 단위로 저장하는 메인 메소드
        1. API에서 전체 기업 목록을 ZIP 파일로 받아옴
        2. ZIP 파일을 열어서 XML 데이터 파싱
        3. 배치 단위로 나누어서 처리
        """
        # API에서 기업 코드 데이터 받아오기
        corp_codes = self._get_corp_code()
        if not corp_codes:
            return

        # ZIP 파일 처리
        with io.BytesIO(corp_codes) as zip_stream:
            with zipfile.ZipFile(zip_stream) as zip_file:
                for file_name in zip_file.namelist():
                    with zip_file.open(file_name) as xml_file:
                        # XML 데이터를 딕셔너리로 변환
                        companies_data = xmltodict.parse(xml_file.read())
                        companies_list = companies_data['result']['list']
                        
                        # 전체 기업 목록을 배치 단위로 나누어 처리
                        for batch_num, i in enumerate(range(0, len(companies_list), self.BATCH_SIZE), 1):
                            batch = companies_list[i:i + self.BATCH_SIZE]
                            self._process_batch(batch, batch_num)
                            # API 호출 제한 준수를 위한 대기
                            time.sleep(60)  # 1분 대기

    def _process_batch(self, companies, batch_num):
        """
        단일 배치의 기업들을 처리하는 메소드
        
        Args:
            companies (list): 처리할 기업 목록
            batch_num (int): 현재 배치 번호
        """
        batch_data = []
        # 진행률 표시와 함께 각 기업 처리
        for company in tqdm(companies, desc=f"배치 {batch_num} 처리중"):
            try:
                # 기업의 상세 정보 조회
                reg_number = self._get_company_registration_number(company['corp_code'])
                if reg_number:
                    batch_data.append({
                        'corp_code': company['corp_code'],
                        'registration_number': reg_number
                    })
                # API 호출 간 딜레이
                time.sleep(0.1)  # 100ms 대기
            except Exception as e:
                self.logger.error(f"처리 중 오류: {str(e)}")

        # 배치 데이터 저장
        self._save_batch(batch_data, batch_num)

class DartFinancialCollector:
    """수집된 기업 코드를 바탕으로 재무제표 정보를 수집하는 클래스"""
    
    def __init__(self):
        # 기본 데이터 저장 경로
        self.OUTPUT_DIR = 'data/dart'
        # 재무제표 데이터 저장 경로
        self.FINANCIAL_DIR = f"{self.OUTPUT_DIR}/financial_statements"
        # 로깅 설정 초기화
        self._setup_logging()
        # 재무제표 저장 디렉토리 생성
        os.makedirs(self.FINANCIAL_DIR, exist_ok=True)

    def collect_financials_for_batch(self, batch_num):
        """
        특정 배치에 속한 기업들의 재무제표를 수집하는 메소드
        
        Args:
            batch_num (int): 처리할 배치 번호
        """
        # 해당 배치의 최신 파일 찾기
        batch_file = self._get_latest_batch_file(batch_num)
        if not batch_file:
            self.logger.error(f"배치 {batch_num}의 파일을 찾을 수 없습니다.")
            return

        # 배치 파일에서 기업 목록 로드
        with open(batch_file, 'r', encoding='utf-8') as f:
            companies = json.load(f)

        # 각 기업의 재무제표 처리
        for company in tqdm(companies, desc=f"배치 {batch_num} 재무제표 수집"):
            # 출력 파일 경로 설정
            output_file = f"{self.FINANCIAL_DIR}/{company['corp_code']}_financial_statements.json"
            
            # 이미 처리된 파일은 건너뛰기
            if os.path.exists(output_file):
                continue

            try:
                # 재무제표 데이터 수집
                financial_data = self._get_financial_statements(company['registration_number'])
                if financial_data:
                    # 수집된 데이터 저장
                    self._save_financial_data(company, financial_data, output_file)
                # API 호출 제한 준수를 위한 대기
                time.sleep(0.5)
            except Exception as e:
                self.logger.error(f"오류 발생: {str(e)}")

def main():
    """
    메인 실행 함수
    1. 기업 코드 수집 및 저장
    2. 특정 배치의 재무제표 수집
    """
    # 1단계: 기업 코드 수집
    collector = DartCorpCodeCollector()
    collector.collect_corp_codes()

    # 2단계: 재무제표 수집
    financial_collector = DartFinancialCollector()
    batch_to_process = 1  # 처리할 배치 번호 지정
    financial_collector.collect_financials_for_batch(batch_to_process)

if __name__ == "__main__":
    main()
