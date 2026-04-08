# 서울시 문화행사 데이터 정제 파이프라인

서울시 문화행사 정보 CSV를 정제하고 신규 데이터를 Supabase에 적재하는 스크립트입니다.

## 전체 흐름

```plaintext
원본 CSV (이전/현재)
      ↓
  main.py          →  {날짜}_filled.csv   (현재 스냅샷 정제본)
                   →  {날짜}_new.csv      (신규 데이터만 추출)
      ↓
sync_supabase.py   →  Supabase events 테이블 적재
```

## 요구사항

```bash
pip install -r requirements.txt
```

| 패키지                     | 용도                   |
| -------------------------- | ---------------------- |
| pandas                     | CSV 처리               |
| geohash2                   | 위경도 → 지오해시 변환 |
| chardet                    | 인코딩 자동 감지       |
| mysql_connector_repackaged | DB 연결                |

## 파일 네이밍 규칙

원본 CSV 파일명은 아래 형식을 따라야 합니다.

```plaintext
서울시 문화행사 정보({월.일}).csv
```

예시:

- `서울시 문화행사 정보(3.7).csv`
- `서울시 문화행사 정보(4.8).csv`

---

## 1단계 — 데이터 정제 (`main.py`)

### 실행 방법

```bash
python main.py
```

실행하면 일자와 Supabase 적재 여부를 순서대로 입력받습니다.

```
이전 스냅샷 일자를 입력하세요 (예: 3.7): 3.7
현재 스냅샷 일자를 입력하세요 (예: 4.8): 4.8

... (데이터 정제 진행) ...

Supabase에 신규 데이터를 적재하시겠습니까? [y/N]: y
```

- `y` 입력 시 Supabase 적재를 이어서 진행합니다.
- `N` 또는 엔터만 누르면 적재 없이 종료합니다.

### 처리 내용 (데이터 정제)

- 인코딩 자동 감지 및 모지바케(깨진 한글) 복구
- 카테고리·구(區) 레이블을 seq 값으로 매핑
- 위경도 유효성 검사 및 뒤바뀐 좌표 자동 수정
- 지오해시 계산
- 이전 스냅샷 기준 손상 텍스트 복원
- 중복 행 제거 (`detail_url`, `event_name`, `start_date`, `place` 기준)
- 이전 스냅샷에 없는 신규 행만 별도 추출

---

## Supabase 환경변수 설정

Supabase 적재를 사용하는 경우 프로젝트 루트의 `.env` 파일에 아래 값을 채웁니다.

```
SUPABASE_URL=https://xxxx.supabase.co
SUPABASE_SERVICE_ROLE_KEY=your_service_role_key
```

`sync_supabase.py`를 단독으로 실행할 수도 있습니다.

```bash
python sync_supabase.py --input "서울시 문화행사 정보(4.8)_new.csv"
```

| 옵션           | 기본값                              | 설명                        |
| -------------- | ----------------------------------- | --------------------------- |
| `--input`      | `서울시 문화행사 정보(3.7)_new.csv` | 적재할 신규 데이터 CSV 경로 |
| `--env-file`   | `./.env`                            | 환경변수 파일 경로          |
| `--table`      | `events`                            | 적재 대상 테이블명          |
| `--batch-size` | `500`                               | 회당 insert 레코드 수       |

---

## 전체 실행 예시

```bash
python main.py
```

---

## 프로젝트 구조

```plaintext
.
├── main.py                 # 데이터 정제 스크립트
├── sync_supabase.py        # Supabase 적재 스크립트
├── check_mapping.py        # 카테고리·구 매핑 검증 유틸
├── requirements.txt
├── .env                    # Supabase 환경변수 (git 제외)
├── enums/
│   ├── category.py         # 카테고리 레이블/seq enum
│   └── gu.py               # 구(區) 레이블/seq enum
└── utils/
    ├── enum_mapping.py     # enum 매핑 헬퍼
    └── geohash_calc.py     # 지오해시 계산
```
