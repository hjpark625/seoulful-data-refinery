import pandas as pd
import chardet
from pathlib import Path
from enums.category import CategorySeq, CategoryLabel
from enums.gu import GuSeq, GuLabel
from utils.enum_mapping import get_enum_seq
from utils.geohash_calc import calculate_geohash

# 최초 데이터 파일
file_path = Path("./data_name.csv")

# 원본 column key 맵핑 최초 csv파일 인코딩 문제로 인해 잠시 보류
# column_key_mapping = {
#     "분류": "category_seq",
#     "자치구": "gu_seq",
#     "공연/행사명": "event_name",
#     "날짜/기간": "period",
#     "장소": "place",
#     "기관명": "org_name",
#     "이용대상": "use_target",
#     "이용요금": "ticket_price",
#     "출연자정보": "player",
#     "프로그램소개": "describe",
#     "기타내용": "etc_desc",
#     "홈페이지 주소": "homepage_link",
#     "대표이미지": "main_img",
#     "신청일": "reg_date",
#     "시민/기관": "is_public",
#     "시작일": "start_date",
#     "종료일": "end_date",
#     "테마분류": "theme",
#     "위도(X좌표)": "latitude",
#     "경도(Y좌표)": "longitude",
#     "유무료": "is_free",
#     "문화포털상세URL": "detail_url",
# }

# 파일 인코딩 확인
with open(file_path, "rb") as f:
    result = chardet.detect(f.read())
    detected_encoding = result["encoding"]
    print(f"파일 인코딩 확인 단계에서 감지된 인코딩 형식: {detected_encoding}")

# CSV 파일 로드
try:
    df = pd.read_csv(file_path, encoding="UTF-8")
    # df = pd.read_csv(file_path, encoding=detected_encoding)
    # print(f"감지된 {detected_encoding} 형식으로 CSV 파일을 성공적으로 로드했습니다.")
    print("UTF-8로 인코딩 성공")
except Exception:
    try:
        df = pd.read_csv(file_path, encoding="EUC-KR")
        print("EUC-KR로 인코딩된 파일")
    except Exception:
        df = pd.read_csv(file_path, encoding=detected_encoding)
        print(
            f"감지된 {detected_encoding} 형식으로 CSV 파일을 성공적으로 로드했습니다."
        )


# 원본 컬럼 이름 확인
# print("Original Columns:", df.columns.tolist())

# column key 맵핑
# df = df.rename(columns=column_key_mapping)

# 컬럼 이름 변경 후 확인
# print("Renamed Columns:", df.columns.tolist())

# 데이터 내부의 공백은 Null 처리
df = df.fillna("NULL")

# event_id 삽입 과정
df.insert(0, "event_id", range(1, len(df) + 1))

# latitude, longitude를 토대로 geohash 계산 후 새로운 칼럼 추가
df["geohash"] = df.apply(lambda row: calculate_geohash(row), axis=1)

# Enum 맵핑
category_label_to_seq = {
    label.value: seq.value for label, seq in zip(CategoryLabel, CategorySeq)
}
gu_label_to_seq = {label.value: seq.value for label, seq in zip(GuLabel, GuSeq)}

# category_seq로 변환
df["category_seq"] = df.apply(
    lambda row: get_enum_seq(row["category_seq"], category_label_to_seq), axis=1
)
# gu_seq로 변환
df["gu_seq"] = df.apply(
    lambda row: get_enum_seq(row["gu_seq"], gu_label_to_seq), axis=1
)
# is_public, is_free를 boolean으로 변환
df["is_public"] = df["is_public"].apply(lambda row: True if row == "기관" else False)
df["is_free"] = df["is_free"].apply(lambda row: True if row == "무료" else False)

# 변환완료 후 새로운 csv 파일로 저장
df.to_csv(
    "./data_name_filled.csv",
    index=False,
    na_rep="NULL",
    encoding="utf-8-sig",
)
