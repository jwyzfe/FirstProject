import requests
import io
import zipfile
import xml.etree.ElementTree as ET
import xmltodict
import json


class dart_financials_class:


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
        if response.status_code == 200: # 성공적으로 응답 받음
            return response.content
        else: # 오류 발생 시 상태 코드와 오류 메시지 출력
            print(f"Error: {response.status_code}")
            print(response.text)
            return None
        
    def get_cor_first_info(corp_code):
        # 요청 URL과 인증키 설정
        first_list = [] 
        url = "https://opendart.fss.or.kr/api/company.json"
        api_key = "19307cd5ad804dd73f156678b8b4cffa1d83122e"
        # 파라미터 설정
        params = {
             
                'crtfc_key' : api_key, 
                'corp_code' : corp_code
            }
            
        # 요청 보내기
        response = requests.get(url, params=params)
            
        # 응답 상태 확인
            
        if response.status_code == 200: # 값이 오류가 나지 않으면
            contents = json.loads(response.text)
            first_list.append(contents)
            final_of_first_list = first_list['jurir_no']
            pass
        return final_of_first_list
    
    def get_cor_second_info(final_of_first_list):
        # 요청 URL과 인증키 설정
        second_list = [] 
        for year in range(2017, 2023, 1):
            url = "http://apis.data.go.kr/1160100/service/GetFinaStatInfoService_V2/getBs_V2"
            api_key = "stCwIy+LoSZpTsxJ4usyyYV3yWNUePAIHISVLlDaFUMfDQDW77n8maQ0pmOH4aJy25qsGso7nLjNXokXaJDGlQ=="
            # 파라미터 설정
            params = {
                'numOfRows': '1', 'pageNo' : '1', 'resultType' : 'json', 
                'serviceKey' : api_key, 
                'crno' : final_of_first_list, 'bizYear' : str(year)
            }
            
            # 요청 보내기
            response = requests.get(url, params=params)
            
            # 응답 상태 확인
            
            if response.status_code == 200: # 값이 오류가 나지 않으면
                final_contents = json.loads(response.text)
                second_list.append(final_contents)
                pass    

        return second_list.append(final_contents)
        
    def file_down(): # run
        zip_data = dart_financials_class.get_corp_code()
        cor_code_list = []
        if zip_data:
            with io.BytesIO(zip_data) as zip_stream:
                with zipfile.ZipFile(zip_stream) as zip_file:
                    for file_name in zip_file.namelist():
                        with zip_file.open(file_name) as xml_file:
                            xml_content = xml_file.read()
                            # XML을 딕셔너리로 변환
                            xml_dict = xmltodict.parse(xml_content)
                            for dict in xml_dict['result']['list']:
                            # corp_code = xml_dict['list'][a]['corp_code']
                                corp_code = dict['corp_code']
                                # # 딕셔너리를 JSON으로 변환
                                # json_data = json.dumps(corp_code, indent=4, ensure_ascii=False)
                                # # JSON 파일로 저장
                                # with open("test.json", "w", encoding='utf-8') as f:
                                #     f.write(json_data)
                                #     print("JSON 파일이 test.json으로 저장")
                                cor_code_list.append(dart_financials_class.get_cor_first_info(corp_code))

        return



           
if __name__ == "__main__":
    test_list = dart_financials_class.file_down()
    pass