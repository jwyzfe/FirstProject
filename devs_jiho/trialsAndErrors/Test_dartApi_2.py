# 필요한 모듈 임포트
import requests  # HTTP 요청을 위한 모듈
import io  # 입출력 처리를 위한 모듈
import zipfile  # ZIP 파일 처리를 위한 모듈
import xml.etree.ElementTree as ET  # XML 파싱을 위한 모듈
import xmltodict  # XML을 딕셔너리로 변환하기 위한 모듈
import json  # JSON 데이터 처리를 위한 모듈
from tqdm import tqdm  # 진행률 표시를 위한 모듈
import logging  # 로깅을 위한 모듈
import time  # 시간 지연을 위한 모듈
from datetime import datetime  # 날짜/시간 처리를 위한 모듈
import os  # 파일/디렉토리 조작을 위한 모듈
import glob  # 파일 패턴 매칭을 위한 모듈

class DartCorpCodeCollector:
    """DART API를 통해 기업 코드를 수집하고 저장하는 클래스"""
    
    def __init__(self):
        # 한 번에 처리할 기업 수 설정 (API 호출 제한 고려)
        self.BATCH_SIZE = 10000
        # 데이터를 저장할 기본 디렉토리 경로
        self.OUTPUT_DIR = 'data/dart'
        # DART API 인증키 설정
        self.DART_API_KEY = "19307cd5ad804dd73f156678b8b4cffa1d83122e"
        # 로깅 설정 초기화
        self._setup_logging()
        # 출력 디렉토리가 없으면 생성
        os.makedirs(self.OUTPUT_DIR, exist_ok=True)

    def _setup_logging(self):
        """로깅 설정을 초기화하는 메소드"""
        # 로깅 기본 설정
        logging.basicConfig(
            level=logging.INFO,  # 로그 레벨을 INFO로 설정
            format='%(asctime)s - %(levelname)s - %(message)s',  # 로그 메시지 형식
            datefmt='%Y-%m-%d %H:%M:%S'  # 날짜 형식
        )
        # 로거 인스턴스 생성
        self.logger = logging.getLogger(__name__)

    def _get_corp_code(self):
        """DART API에서 기업 코드 데이터를 요청하는 메소드"""
        # 로그 메시지 출력
        self.logger.info("DART API에서 기업 코드 데이터 요청 중...")
        
        # API 엔드포인트 URL
        url = "https://opendart.fss.or.kr/api/corpCode.xml"
        # API 요청 파라미터
        params = {'crtfc_key': self.DART_API_KEY}
        
        # API 요청 보내기
        response = requests.get(url, params=params)
        
        # 응답 상태 확인
        if response.status_code == 200:  # 성공적인 응답
            self.logger.info("기업 코드 데이터 성공적으로 수신")
            return response.content
        else:  # 오류 발생
            self.logger.error(f"DART API 오류: {response.status_code}")
            self.logger.error(response.text)
            return None

    def _get_company_registration_number(self, corp_code):
        """
        기업의 상세 정보를 조회하는 메소드
        
        Args:
            corp_code (str): 기업 코드
            
        Returns:
            dict or None: 기업 정보 딕셔너리 또는 None
        """
        # API 엔드포인트 URL
        url = "https://opendart.fss.or.kr/api/company.json"
        # API 요청 파라미터
        params = {
            'crtfc_key': self.DART_API_KEY,
            'corp_code': corp_code
        }
        
        # API 요청 보내기
        response = requests.get(url, params=params)
        
        # 응답 처리
        if response.status_code == 200:
            company_info = json.loads(response.text)
            corp_cls = company_info.get('corp_cls')
            # Y값을 가진 기업만 반환 (상장기업)
            if corp_cls == 'Y':
                return company_info
        return None
    
    def _save_batch(self, batch_data, batch_num):
        """
        배치 데이터를 JSON 파일로 저장하는 메소드
        Args:
            batch_data (list): 저장할 기업 데이터 리스트
            batch_num (int): 배치 번호
        """
        # 현재 시간을 파일명에 포함하기 위한 타임스탬프 생성
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            # 저장할 파일 경로 생성
        filename = f"{self.OUTPUT_DIR}/registration_numbers_batch_{batch_num}_{timestamp}.json"
            
            # JSON 파일로 데이터 저장
        with open(filename, 'w', encoding='utf-8') as f:
                json.dump(batch_data, f, ensure_ascii=False, indent=2)
            
            # 저장 완료 로그
        self.logger.info(f"배치 {batch_num} 결과가 {filename}에 저장되었습니다.")

    def collect_corp_codes(self):
        """기업 코드를 수집하고 배치 단위로 저장하는 메소드"""
        # ZIP 파일 데이터 받기
        zip_data = self._get_corp_code()
        if not zip_data:
            self.logger.error("기업 코드 데이터를 받아오는데 실패했습니다.")
            return

        # ZIP 파일 메모리에서 읽기
        with zipfile.ZipFile(io.BytesIO(zip_data)) as zip_file:
            # XML 파일 읽기
            xml_data = zip_file.read('CORPCODE.xml')
            # XML을 딕셔너리로 변환
            corp_data = xmltodict.parse(xml_data)
            # 기업 목록 추출
            corps = corp_data['result']['list']

        # 배치 처리를 위한 변수들
        batch_data = []
        batch_num = 1
        
        # 진행률 표시와 함께 기업 정보 처리
        for corp in tqdm(corps, desc="기업 정보 처리 중"):
            corp_code = corp.get('corp_code')
            if corp_code:
                # 기업 상세 정보 조회
                company_info = self._get_company_registration_number(corp_code)
                if company_info:
                    batch_data.append(company_info)

            # 배치 크기에 도달하면 저장
            if len(batch_data) >= self.BATCH_SIZE:
                self._save_batch(batch_data, batch_num)
                batch_data = []
                batch_num += 1

        # 남은 데이터가 있으면 저장
        if batch_data:
            self._save_batch(batch_data, batch_num)

        self.logger.info("기업 코드 수집 완료")

class DartFinancialCollector:
    """수집된 기업 코드를 바탕으로 재무제표 정보를 수집하는 클래스"""
    
    def __init__(self):
        # 기본 데이터 저장 경로
        self.OUTPUT_DIR = 'data/dart'
        # 재무제표 데이터 저장 경로
        self.FINANCIAL_DIR = f"{self.OUTPUT_DIR}/financial_statements"
        # 금융감독원 API 키 설정
        self.FINANCIAL_API_KEY = "stCwIy%2BLoSZpTsxJ4usyyYV3yWNUePAIHISVLlDaFUMfDQDW77n8maQ0pmOH4aJy25qsGso7nLjNXokXaJDGlQ%3D%3D"
        # 로깅 설정 초기화
        self._setup_logging()
        # 재무제표 저장 디렉토리 생성
        os.makedirs(self.FINANCIAL_DIR, exist_ok=True)

    def _get_financial_statements(self, registration_number):
        """
        기업의 재무제표 데이터를 수집하는 메소드
        
        Args:
            registration_number (str): 기업 등록번호
            
        Returns:
            list: 연도별 재무제표 데이터 리스트
        """
        # 재무제표 데이터를 저장할 리스트
        financial_statements = []
        # 금융위원회 API 엔드포인트
        url = "http://apis.data.go.kr/1160100/service/GetFinaStatInfoService_V2/getBs_V2"
        
        # 로그 메시지 출력
        self.logger.info(f"기업 번호 {registration_number}의 재무제표 조회 중...")
        
        # 현재 연도까지의 데이터 수집
        current_year = datetime.now().year
        for year in tqdm(range(2017, current_year + 1), desc="연도별 데이터 수집"):
            # API 요청 파라미터 설정
            params = {
                'numOfRows': '1',  # 한 페이지당 결과 수
                'pageNo': '1',     # 페이지 번호
                'resultType': 'json',  # 응답 데이터 형식
                'serviceKey': self.FINANCIAL_API_KEY,  # API 키
                'crno': registration_number,  # 기업 등록번호
                'bizYear': str(year)  # 사업연도
            }
            
            # API 요청 보내기
            response = requests.get(url, params=params)
            
            # 응답 처리
            if response.status_code == 200:
                financial_data = json.loads(response.text)
                financial_statements.append(financial_data)
            else:
                self.logger.warning(f"{year}년도 데이터 조회 실패")

        return financial_statements

    def _save_financial_data(self, company, financial_data, output_file):
        """
        재무제표 데이터를 JSON 파일로 저장하는 메소드
        
        Args:
            company (dict): 기업 정보
            financial_data (list): 재무제표 데이터
            output_file (str): 저장할 파일 경로
        """
        # 저장할 데이터 구조화
        result = {
            'corp_code': company['corp_code'],
            'registration_number': company['registration_number'],
            'financial_statements': financial_data
        }
        
        # JSON 파일로 저장
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        # 저장 완료 로그
        self.logger.info(f"기업 코드 {company['corp_code']}의 재무제표가 저장되었습니다.")

    def _get_latest_batch_file(self, batch_num):
        """
        특정 배치의 가장 최신 파일을 찾는 메소드
        
        Args:
            batch_num (int): 배치 번호
            
        Returns:
            str or None: 최신 파일 경로 또는 None
        """
        # 해당 배치 번호의 모든 파일 찾기
        files = glob.glob(f"{self.OUTPUT_DIR}/registration_numbers_batch_{batch_num}_*.json")
        # 가장 최근 파일 반환 (없으면 None)
        return max(files, key=os.path.getctime) if files else None

def main():
    """메인 실행 함수"""
    try:
        # 1단계: 기업 코드 수집
        collector = DartCorpCodeCollector()
        collector.collect_corp_codes()

        # 2단계: 재무제표 수집
        financial_collector = DartFinancialCollector()
        batch_to_process = 1  # 처리할 배치 번호 지정
        financial_collector.collect_financials_for_batch(batch_to_process)
        
    except Exception as e:
        logging.error(f"프로그램 실행 중 오류 발생: {str(e)}")

if __name__ == "__main__":
    main()