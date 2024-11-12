# 프로젝트 이름

이 프로젝트는 데이터 스크래핑 및 API 요청을 처리하는 스케줄러를 구현한 것입니다. 이 README 파일은 프로젝트 구조와 작성 방법에 대한 설명을 제공합니다.

## 프로젝트 구조
```
프로젝트 폴더/
│
├── commons/
│ ├── init.py
│ ├── mongo_insert_recode.py # MongoDB에 데이터를 삽입하는 기능
│ ├── api_send_requester.py # API 요청을 보내는 기능
│ ├── sel_iframe_courtauction.py # 특정 iframe에서 데이터를 추출하는 기능
│ └── bs4_do_scrapping.py # BeautifulSoup을 이용한 스크래핑 기능
│
├── devs/
│ ├── init.py
│ └── api_test_class.py # commons를 상속받아 API 테스트 기능 구현
│
└── schedulers/
├── init.py
└── main.py # 스케줄러와 메인 로직을 포함
```


## 폴더 설명

### commons
- 이 폴더는 공통적으로 사용되는 기능들을 포함합니다.
- 각 파일은 특정 기능에 대한 클래스를 정의하거나 함수를 제공합니다.
  - `mongo_insert_recode.py`: MongoDB에 데이터를 삽입하는 기능을 구현합니다.
  - `api_send_requester.py`: API 요청을 보내는 기능을 구현합니다.
  - `sel_iframe_courtauction.py`: 특정 iframe에서 데이터를 추출하는 기능을 구현합니다.
  - `bs4_do_scrapping.py`: BeautifulSoup을 이용한 웹 스크래핑 기능을 구현합니다.

### devs
- `commons`에서 제공하는 기능을 상속받는 느낌으로 더 구체적인 기능을 구현합니다.
- 예를 들어, `api_test_class.py`에서는 API 테스트를 위한 클래스를 만들고, `commons`의 기능을 활용하여 필요한 로직을 추가합니다.

### schedulers
- `main.py` 파일은 프로그램의 진입점으로, `devs`와 `commons`에서 구현한 기능들을 등록하고 실행하는 역할을 합니다.
- 스케줄러를 설정하고, 주기적으로 작업을 수행하도록 합니다. MongoDB에 로그를 저장하는 기능도 포함되어 있습니다.

## 작성 방법

1. **devs** 폴더에서 `commons`의 기능을 import하여 구체적인 로직을 추가합니다.
2. **schedulers/main.py**에서 **devs** 에서 작성한 함수를 스케줄러에 등록하여 주기적으로 작업을 수행합니다.

## 실행 방법

1. 필요한 패키지를 설치합니다.
   ```bash
   pip install -r requirements.txt
   ```

2. `main.py`를 실행하여 스케줄러를 시작합니다.
   ```bash
   python schedulers/main.py
   ```

## 라이센스

생각해보기