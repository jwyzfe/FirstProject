import pandas as pd

# 1. CSV 파일 로드
try:
    # 기본 UTF-8 인코딩으로 시도
    df = pd.read_csv('kospi_all.csv', encoding='utf-8')  
except UnicodeDecodeError as e:
    # UTF-8로 읽을 수 없다면 다른 인코딩을 시도
    print(f"UTF-8로 읽을 수 없습니다. 오류: {e}. 다른 인코딩을 시도합니다.")
    df = pd.read_csv('kospi_all.csv', encoding='latin1')  # latin1 또는 ISO-8859-1로 시도

# 2. 인코딩 문제 해결 후 데이터 출력
print("데이터 로드 완료:")
print(df.head())

# 3. '우선주'라는 단어가 포함된 컬럼을 찾아 제거
df_cleaned = df.loc[:, ~df.columns.str.contains('우선주')]  # '우선주'가 포함된 컬럼 제거

# 4. 결과를 새로운 CSV 파일로 저장
df_cleaned.to_csv('cleaned_kospi_all.csv', index=False)

print("'우선주'가 포함된 컬럼이 제거된 새로운 파일이 'cleaned_kospi_all.csv'로 저장되었습니다.")
