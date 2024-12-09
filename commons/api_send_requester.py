import requests
import json
from typing import Optional, List, Dict, Union, Any

class ApiRequester:
    """API 요청을 처리하는 클래스"""

    @staticmethod
    def send_api(
        base_url: str, 
        params: Dict[str, Any], 
        keys: Optional[List[str]] = None
    ) -> Optional[Union[List[Dict], Dict, Any]]:
        """API 엔드포인트로 GET 요청을 보내고 결과를 처리

        Args:
            base_url (str): API 기본 URL
            params (Dict[str, Any]): API 요청 파라미터
                - URL 쿼리 파라미터로 변환될 딕셔너리
            keys (Optional[List[str]], optional): 응답에서 추출할 키 목록. Defaults to None.
                - None인 경우 전체 응답 반환
                - 키 목록이 주어진 경우 해당 키의 값만 추출하여 반환

        Returns:
            Optional[Union[List[Dict], Dict, Any]]: 
                - List[Dict]: keys가 지정된 경우, 지정된 키들의 값만 포함하는 딕셔너리 리스트
                - Dict: 단일 응답이고 keys가 None인 경우
                - Any: 기타 JSON 응답 형식
                - None: 요청 실패 또는 빈 응답

        Note:
            - 응답 상태 코드가 200이 아닌 경우 None 반환
            - 빈 응답('[]')의 경우 None 반환
            - 단일 항목 응답의 경우 리스트로 변환하여 처리

        Example:
            ```python
            # 전체 응답 받기
            result = ApiRequester.send_api("https://api.example.com", {"param": "value"})

            # 특정 키만 추출
            result = ApiRequester.send_api(
                "https://api.example.com", 
                {"param": "value"},
                keys=["id", "name"]
            )
            ```
        """
        response = requests.get(base_url, params=params)
        print(response.status_code)

        if response.status_code == 200:
            if response.text != '[]':
                try:
                    content = json.loads(response.content)

                    if keys is None:
                        return content
                    else:
                        result_list = []

                        # 단일 항목을 리스트로 변환
                        if not isinstance(content, list):
                            content = [content]

                        # 지정된 키만 추출
                        for con in content:
                            item_dict = {key: con[key] for key in keys}
                            result_list.append(item_dict)

                        return result_list

                except json.JSONDecodeError as e:
                    print(f"JSON 파싱 오류: {e}")
                    return None
                    
            else:
                print(f"error : result empty {response.content}")
                return None
        else:
            print(f"error : {response.status_code}")
            return None