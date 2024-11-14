import requests

def get_sec_filings(cik):
    # SEC EDGAR API URL
    url = f'https://data.sec.gov/submissions/CIK{cik}.json'
    
    # API 요청
    headers = {
        'User-Agent': 'SangHoon Lee (demonic0319@gmail.com)'
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        filings = data.get('filings', {}).get('recent', {})
        
        # 10-Q 보고서 검색 및 다운로드
        for index in range(len(filings.get('form', []))):
            if filings['form'][index] == '10-Q':
                print(f"10-Q Report:")
                print(f"Date: {filings['filingDate'][index]}")
                print(f"Link: {filings['primaryDocument'][index]}")
                print(f"Description: {filings['primaryDocDescription'][index]}")
                
                # 10-Q 보고서의 링크를 사용하여 API 요청
                report_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{filings['accessionNumber'][index].replace('-', '')}/{filings['primaryDocument'][index]}"
                report_response = requests.get(report_url, headers=headers)
                
                if report_response.status_code == 200:
                    # 보고서 파일 저장
                    report_filename = filings['primaryDocument'][index]
                    with open(report_filename, 'wb') as file:
                        file.write(report_response.content)
                    print(f"Successfully fetched and saved 10-Q report as: {report_filename}")
                else:
                    print(f"Error fetching 10-Q report: {report_response.status_code}")
                print()
    else:
        print(f"Error: {response.status_code}")

# CIK 번호를 사용하여 보고서 가져오기
cik_number = '0001760689'  # Apple Inc.의 CIK 번호
get_sec_filings(cik_number)