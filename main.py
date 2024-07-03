import pandas as pd
import chardet
from pathlib import Path
from enums.category import CategorySeq, CategoryLabel
from enums.gu import GuSeq, GuLabel
from utils.enum_mapping import get_enum_seq
from utils.geohash_calc import calculate_geohash

# 최초 데이터 파일
file_path = Path("./서울시 문화행사 정보(7.2).csv")

# Enum 맵핑
category_label_to_seq = {
    label.value: seq.value for label, seq in zip(CategoryLabel, CategorySeq)
}
gu_label_to_seq = {label.value: seq.value for label, seq in zip(GuLabel, GuSeq)}

# 파일 인코딩 확인
with open(file_path, "rb") as f:
    result = chardet.detect(f.read())
    encoding = result["encoding"]

df = pd.read_csv(file_path, encoding=encoding)


# 데이터 내부의 공백은 Null 처리
df = df.fillna("NULL")

# latitude, longitude를 토대로 geohash 계산 후 새로운 칼럼 추가
df["geohash"] = df.apply(lambda row: calculate_geohash(row), axis=1)
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
    "./서울시 문화행사 정보(7.2)_filled.csv",
    index=False,
    na_rep="NULL",
    encoding="utf-8-sig",
)
