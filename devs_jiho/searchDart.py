import json
from pathlib import Path
from typing import List, Dict

class RegistrationHandler:
    def __init__(self):
        self.data_path = Path("data/dart")
        self.registration_data: List[Dict[str, str]] = []
    
    def load_registration_files(self) -> List[Dict[str, str]]:
        """registration으로 시작하는 모든 JSON 파일을 로드합니다"""
        for json_file in self.data_path.glob("registration*.json"):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.registration_data.extend(data)
            except Exception as e:
                print(f"파일 읽기 오류 {json_file}: {str(e)}")
        
        return self.registration_data
    
    def get_registration_by_corp_code(self, corp_code: str) -> str:
        """법인 코드로 사업자등록번호를 조회합니다"""
        for item in self.registration_data:
            if item["corp_code"] == corp_code:
                return item["registration_number"]
        return ""
    
    def get_corp_code_by_registration(self, registration: str) -> str:
        """사업자등록번호로 법인 코드를 조회합니다"""
        for item in self.registration_data:
            if item["registration_number"] == registration:
                return item["corp_code"]
        return ""

# 메인 코드에서 사용할 때
handler = RegistrationHandler()
data = handler.load_registration_files()

# # 법인 코드로 사업자등록번호 조회
# registration_number = handler.get_registration_by_corp_code("00434003")
# print(registration_number)  # 출력: "1615110021778"

# # 사업자등록번호로 법인 코드 조회
# corp_code = handler.get_corp_code_by_registration("1615110021778")
# print(corp_code)  # 출력: "00434003"