# # # import requests
# # # import io
# # # import zipfile

# # # def get_corp_code():
# # #     # 요청 URL과 인증키 설정
# # #     url = "https://opendart.fss.or.kr/api/corpCode.xml"
# # #     api_key = "19307cd5ad804dd73f156678b8b4cffa1d83122e"
    
# # #     # 파라미터 설정
# # #     params = {
# # #         'crtfc_key': api_key
# # #     }
    
# # #     # 요청 보내기
# # #     response = requests.get(url, params=params)
    
# # #     # 응답 상태 확인
# # #     if response.status_code == 200:
# # #         # 성공적으로 응답 받음
# # #         return response.content  # XML 형식의 응답 반환
# # #     else:
# # #         # 오류 발생 시 상태 코드와 오류 메시지 출력
# # #         print(f"Error: {response.status_code}")
# # #         print(response.text)
# # #         return None

# # # # 함수 실행 예제
# # # if __name__ == "__main__":
# # #     # xml_data = get_corp_code()
# # #     zip_stream = get_corp_code()

# # # # ZIP 스트림을 파일로 저장
# # #     with open('output.zip', 'wb') as f:
# # #         f.write(zip_stream.getvalue())

# # # #    if xml_data:
# # # #       print(xml_data)


# # #----

# import requests
# import io
# import zipfile

# def get_corp_code():
#     # 요청 URL과 인증키 설정
#     url = "https://opendart.fss.or.kr/api/corpCode.xml"
#     api_key = "19307cd5ad804dd73f156678b8b4cffa1d83122e"
    
#     # 파라미터 설정
#     params = {
#         'crtfc_key': api_key
#     }
    
#     # 요청 보내기
#     response = requests.get(url, params=params)
    
#     # 응답 상태 확인
#     if response.status_code == 200:
#         # 성공적으로 응답 받음
#         return response.content  # ZIP 파일 형식의 응답 반환
#     else:
#         # 오류 발생 시 상태 코드와 오류 메시지 출력
#         print(f"Error: {response.status_code}")
#         print(response.text)
#         return None

# # 함수 실행 예제
# if __name__ == "__main__":
#     zip_data = get_corp_code()

#     # ZIP 데이터를 메모리로 로드하여 XML 파일 추출
#     if zip_data:
#         with io.BytesIO(zip_data) as zip_stream:
#             with zipfile.ZipFile(zip_stream) as zip_file:
#                 # ZIP 파일 내부의 XML 파일 이름 확인 및 추출
#                 for file_name in zip_file.namelist():
#                     # XML 파일 읽기
#                     with zip_file.open(file_name) as xml_file:
#                         xml_content = xml_file.read()
#                         print(xml_content.decode('utf-8'))  # XML 파일 내용 출력 (utf-8로 디코딩)
# # test.xml 파일로 저장
#                         with open("test.xml", "wb") as f:
#                             f.write(xml_content)
#                         print("XML 파일이 test.xml로 저장되었습니다.")

# #--------------------


import requests
import io
import zipfile
import xml.etree.ElementTree as ET
import xmltodict
import json

def get_corp_code():
    # 요청 URL과 인증키 설정
    url = "https://opendart.fss.or.kr/api/corpCode.xml"
    api_key = "19307cd5ad804dd73f156678b8b4cffa1d83122e"
    
    # 파라미터 설정
    params = {
        'crtfc_key': api_key
    }
    
    # 요청 보내기
    response = requests.get(url, params=params)
    
    # 응답 상태 확인
    if response.status_code == 200:
        # 성공적으로 응답 받음
        return response.content  # ZIP 파일 형식의 응답 반환
    else:
        # 오류 발생 시 상태 코드와 오류 메시지 출력
        print(f"Error: {response.status_code}")
        print(response.text)
        return None

# 함수 실행 예제
if __name__ == "__main__":
    zip_data = get_corp_code()

    # ZIP 데이터를 메모리로 로드하여 XML 파일 추출
    #-----
#     if zip_data:
#         with io.BytesIO(zip_data) as zip_stream:
#             with zipfile.ZipFile(zip_stream) as zip_file:
#                 # ZIP 파일 내부의 XML 파일 이름 확인 및 추출
#                 for file_name in zip_file.namelist():
#                     # XML 파일 읽기
#                     with zip_file.open(file_name) as xml_file:
#                         xml_content = xml_file.read()
#                         # print(xml_content.decode('utf-8'))  # XML 파일 내용 출력 (utf-8로 디코딩)
# # test.xml 파일로 저장
#                         with open("test.xml", "wb") as f:
#                             f.write(xml_content)
    if zip_data:
        with io.BytesIO(zip_data) as zip_stream:
            with zipfile.ZipFile(zip_stream) as zip_file:
                for file_name in zip_file.namelist():
                    with zip_file.open(file_name) as xml_file:
                        xml_content = xml_file.read()

                        # XML을 딕셔너리로 변환
                        xml_dict = xmltodict.parse(xml_content)

                        # 딕셔너리를 JSON으로 변환
                        json_data = json.dumps(xml_dict, indent=4, ensure_ascii=False)

                        # JSON 파일로 저장
                        with open("test.json", "w", encoding='utf-8') as f:
                            f.write(json_data)
                            print("JSON 파일이 test.json으로 저장되었습니다.")
                            # print("XML 파일이 test.xml로 저장되었습니다.")

# # XML 파일을 파싱하고 JSON으로 변환
#root = ET.fromstring(xml_content)

# # JSON으로 변환하기 위한 딕셔너리 생성 (예시)
# data = {}
# for corp in root.findall('corpCode'):
#     corp_code = corp.find('corp_code').text
#     corp_name = corp.find('corp_name').text
#     data[corp_code] = corp_name

# # JSON으로 변환
# json_data = json.dumps(data, indent=4, ensure_ascii=False)

# JSON 파일로 저장
# with open("test.json", "w", encoding='utf-8') as f:
#     f.write(json_data)
#     print("JSON 파일이 test.json으로 저장되었습니다.")

# -----

# ... (기존 코드)

# XML 파일을 파싱하고 JSON으로 변환
# tree = ET.parse('xml_content')
# root = tree.getroot()

# # JSON으로 변환하기 위한 딕셔너리 생성
# data = []
# for corp in root.findall('corpCode'):  # 여기서 corpCode 태그가 맞는지 확인
#     corp_code = corp.find('corp_code').text
#     corp_name = corp.find('corp_name').text

#     # 데이터 검증
#     if corp_code and corp_name:
#         data.append({'corp_code': corp_code, 'corp_name': corp_name})
#     else:
#         print("데이터 누락: corp_code 또는 corp_name 값이 없습니다.")

# # JSON으로 변환
# json_data = json.dumps(data, indent=4, ensure_ascii=False)


# # JSON 파일로 저장
# with open("test.json", "w", encoding='utf-8') as f:
#     f.write(json_data)
#     print("JSON 파일이 test.json으로 저장되었습니다.")

# with open('xml_content.xml', encoding='utf-8') as f:
#     doc = xmltodict.parse(f.read())
                          
# json_data = json.loads(json.dumps(doc))
# print(json_data)