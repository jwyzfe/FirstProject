# import csv
# import json

# # CSV를 딕셔너리로 변환
# sp500_dict = {}
# with open('s&p500_tickers.csv', 'r') as file:
#     csv_reader = csv.DictReader(file)
#     for row in csv_reader:
#         sp500_dict[row['Symbol']] = row['Security']

# # 딕셔너리를 JSON 파일로 저장
# with open('sp500_dict.json', 'w') as json_file:
#     json.dump(sp500_dict, json_file, indent=4)

import csv
import json

# CSV에서 심볼만 리스트로 추출
sp500_symbols = []
with open('s&p500_tickers.csv', 'r') as file:
    csv_reader = csv.DictReader(file)
    for row in csv_reader:
        sp500_symbols.append(row['Symbol'])

# 리스트를 JSON 파일로 저장
with open('sp500_symbols.json', 'w') as json_file:
    json.dump(sp500_symbols, json_file, indent=4)