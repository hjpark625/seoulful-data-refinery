import pandas as pd
import chardet
from pathlib import Path
from enums.category import CategorySeq, CategoryLabel
from enums.gu import GuSeq, GuLabel
from utils.enum_mapping import get_enum_seq
from utils.geohash_calc import calculate_geohash

data_name = "서울시 문화행사 정보(7.26)"
new_data_name = "서울시 문화행사 정보(8.9)"

# 최초 데이터 파일 경로
file_path = Path(f"./{new_data_name}.csv")

# 기존 데이터 파일 경로 (비교용)
existing_file_path = Path(f"./{data_name}.csv")


# 파일 인코딩 확인 함수
def detect_encoding(file_path):
    with open(file_path, "rb") as f:
        result = chardet.detect(f.read())
    return result["encoding"]


# 새 데이터 파일 인코딩 감지 및 로드
detected_encoding = detect_encoding(file_path)
try:
    df = pd.read_csv(file_path, encoding=detected_encoding)
    print(f"새 데이터 파일을 {detected_encoding} 인코딩으로 성공적으로 로드했습니다.")
except Exception as e:
    print(f"새 데이터 파일 로드 실패: {e}")
    df = pd.DataFrame()

# 기존 데이터 파일 로드 (비교용)
if existing_file_path.exists():
    existing_encoding = detect_encoding(existing_file_path)
    try:
        existing_df = pd.read_csv(existing_file_path, encoding=existing_encoding)
        print(
            f"기존 데이터 파일을 {existing_encoding} 인코딩으로 성공적으로 로드했습니다."
        )
    except Exception as e:
        print(f"기존 데이터 파일 로드 실패: {e}")
        existing_df = pd.DataFrame()  # 로드 실패 시 빈 데이터프레임 사용
else:
    existing_df = pd.DataFrame()

# 데이터 내부의 공백은 Null 처리
df = df.fillna("NULL")


# latitude, longitude가 숫자로 변환 가능한지 확인하고 변환 불가능한 행 삭제
def validate_and_convert_lat_lon(df, lat_column="latitude", lon_column="longitude"):
    def convert(value):
        try:
            return float(value)
        except ValueError:
            return None

    df[lat_column] = df[lat_column].apply(convert)
    df[lon_column] = df[lon_column].apply(convert)

    # 유효하지 않은 위도/경도 값이 있는 행 삭제
    df = df.dropna(subset=[lat_column, lon_column])
    return df


# 위도(latitude), 경도(longitude) 변환 및 유효성 확인
df = validate_and_convert_lat_lon(df)


# geohash 계산 시 예외를 처리하고, 문제가 있는 행을 출력
def safe_calculate_geohash(row):
    try:
        return calculate_geohash(row)
    except Exception as e:
        print(
            f"geohash 생성 실패: {e}, latitude: {row['latitude']}, longitude: {row['longitude']}"
        )
        return None


# latitude, longitude를 토대로 geohash 계산 후 새로운 칼럼 추가
df["geohash"] = df.apply(safe_calculate_geohash, axis=1)

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

# 기존 데이터에 event_id가 없을 경우 처리
if "event_id" not in existing_df.columns:
    if not existing_df.empty:
        # 기존 데이터에 event_id 추가
        existing_df.insert(0, "event_id", range(1, len(existing_df) + 1))
    last_event_id = 0
else:
    last_event_id = existing_df["event_id"].max()

# 기존 데이터와 새로운 데이터 비교하여 중복되지 않는 행만 선택
if not existing_df.empty:
    new_data = df[~df.apply(tuple, 1).isin(existing_df.apply(tuple, 1))]
else:
    new_data = df

# 새로운 event_id 삽입 과정
if not new_data.empty:
    new_data.insert(
        0, "event_id", range(last_event_id + 1, last_event_id + len(new_data) + 1)
    )

# 기존 데이터와 합치기
if not existing_df.empty:
    final_df = pd.concat([existing_df, new_data], ignore_index=True)
else:
    final_df = new_data

# 변환 완료 후 새로운 CSV 파일에 저장
output_file_path = f"./{new_data_name}_filled.csv"
final_df.to_csv(
    output_file_path,
    index=False,
    na_rep="NULL",
    encoding="utf-8-sig",
)
print(f"포맷팅된 데이터가 {output_file_path} 파일에 저장되었습니다.")
