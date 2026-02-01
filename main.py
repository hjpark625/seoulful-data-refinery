import pandas as pd
import chardet
from pathlib import Path
from enums.category import CategorySeq, CategoryLabel
from enums.gu import GuSeq, GuLabel
from utils.enum_mapping import get_enum_seq
from utils.geohash_calc import calculate_geohash

data_name = ""
new_data_name = ""

# 최초 데이터 파일 경로
file_path = Path(f"./{new_data_name}.csv")

# 기존 데이터 파일 경로 (비교용)
existing_file_path = Path(f"./{data_name}.csv")


# 파일 스마트 로드 함수 (인코딩 자동 감지 및 모지바케 복구)
def load_csv_smartly(file_path):
    if not file_path.exists():
        return pd.DataFrame()

    encodings = ["cp949", "utf-8", "euc-kr", "latin-1"]
    df = None

    for enc in encodings:
        try:
            temp_df = pd.read_csv(file_path, encoding=enc)
            # 성공적으로 읽히면 데이터 검증
            # category_seq 컬럼이 있다면 내용을 확인하여 깨짐 여부 판단 (휴리스틱)
            if "category_seq" in temp_df.columns:
                sample = temp_df["category_seq"].dropna().astype(str).head(10).tolist()
                # CategoryLabel 값 중 하나라도 포함되어 있는지 확인
                valid_labels = [label.value for label in CategoryLabel]
                # '1' 같은 숫자형태나 '전시/미술' 같은 텍스트 형태가 섞여있을 수 있음
                # 하지만 모지바케가 발생하면 'ÇØ...' 처럼 됨.

                # 단순히 읽기에 성공했다고 넘어가면 안됨.
                # 우선순위: cp949가 성공하면 가장 높음 (한국어 윈도우 기본)
                # utf-8이 성공했는데 모지바케인 경우 -> repair 시도

                # ASCII만 있는 경우 어느 것이든 상관없음.
                pass

            print(f"Loaded with encoding: {enc}")
            df = temp_df
            break
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"Error loading with {enc}: {e}")
            continue

    if df is None:
        print(f"Failed to load {file_path}")
        return pd.DataFrame()

    # 모지바케 복구 로직 (UTF-8로 잘못 읽혀서 저장이 된 파일인 경우)
    # 예: "ÇØ" (Latin-1) -> "해" (CP949)
    # 확인 방법: category_seq가 있는데 valid_labels에 없는 한글 값도 아니고 이상한 문자만 있는 경우
    if "category_seq" in df.columns:
        # CategorySeq matching check
        valid_labels = set(label.value for label in CategoryLabel)

        def is_mojibake(series):
            # 매칭률 계산
            matches = series.isin(valid_labels).sum()
            total = len(series)
            if total == 0:
                return False
            return (matches / total) < 0.1  # 10% 미만 매칭이면 의심

        # 현재 데이터가 string인지 확인
        if df["category_seq"].dtype == object:
            if is_mojibake(df["category_seq"]):
                print("Mojibake detected. Attempting repair...")

                def fix_text(text):
                    if not isinstance(text, str):
                        return text
                    try:
                        # Latin-1로 인코딩 후 CP949로 디코딩
                        return text.encode("latin-1").decode("cp949")
                    except (UnicodeEncodeError, UnicodeDecodeError):
                        return text

                # 모든 Object 컬럼에 대해 복구 시도
                for col in df.select_dtypes(include=["object"]).columns:
                    df[col] = df[col].apply(fix_text)

                print("Repair complete.")

    return df


# 새 데이터 파일 로드
df = load_csv_smartly(file_path)

# 기존 데이터 파일 로드
existing_df = load_csv_smartly(existing_file_path)

# 데이터 내부의 공백은 Null 처리
df = df.fillna("NULL")


# 위도(latitude), 경도(longitude) 변환 및 유효성 확인
def validate_and_convert_lat_lon(df, lat_column="latitude", lon_column="longitude"):
    def convert(value):
        try:
            return float(value)
        except ValueError:
            return None

    df[lat_column] = df[lat_column].apply(convert)
    df[lon_column] = df[lon_column].apply(convert)

    # 위도/경도 값이 뒤바뀐 경우 (예: 위도가 90보다 크고 경도가 90보다 작은 경우) 스왑
    # 한국 좌표계 기준: 위도 ~37, 경도 ~127
    swap_condition = (df[lat_column] > 90) & (df[lon_column] < 90)
    if swap_condition.any():
        print(f"위도/경도 뒤바뀜 감지: {swap_condition.sum()}행 수정")
        df.loc[swap_condition, [lat_column, lon_column]] = df.loc[
            swap_condition, [lon_column, lat_column]
        ].values

    # 유효하지 않은 위도/경도 값이 있는 행 삭제
    df = df.dropna(subset=[lat_column, lon_column])
    return df


# 새 데이터 위도/경도 검증 및 변환
df = validate_and_convert_lat_lon(df)


# geohash 계산 시 예외를 처리하고, 문제가 있는 행을 출력
def safe_calculate_geohash(row):
    try:
        return calculate_geohash(row)
    except Exception as e:
        # print(f"geohash 생성 실패: {e}") # 너무 많은 로그 방지
        return None


# 새 데이터 geohash 추가
df["geohash"] = df.apply(safe_calculate_geohash, axis=1)

# 기존 데이터 처리 (Geohash 누락 및 포맷 확인)
if not existing_df.empty:
    existing_df = validate_and_convert_lat_lon(existing_df)
    if "geohash" not in existing_df.columns or existing_df["geohash"].isnull().any():
        print("기존 데이터에 Geohash 계산 적용 중...")
        existing_df["geohash"] = existing_df.apply(safe_calculate_geohash, axis=1)


# Enum 맵핑
category_label_to_seq = {
    label.value: seq.value for label, seq in zip(CategoryLabel, CategorySeq)
}
gu_label_to_seq = {label.value: seq.value for label, seq in zip(GuLabel, GuSeq)}

# category_seq로 변환
df["category_seq"] = df.apply(
    lambda row: get_enum_seq(
        row["category_seq"], category_label_to_seq, CategorySeq.OTHER.value
    ),
    axis=1,
)
# gu_seq로 변환
df["gu_seq"] = df.apply(
    lambda row: get_enum_seq(row["gu_seq"], gu_label_to_seq, GuSeq.OTHER.value), axis=1
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
    # 중복 비교 기준 컬럼 설정 (Business Key)
    # geohash는 미세한 좌표 차이로 다를 수 있으므로 제외
    # 위도/경도도 부동소수점 이슈로 제외하는 것이 안전
    subset_cols = [
        "event_name",
        "start_date",
        "end_date",
        "org_name",
        "place",
    ]
    # 실제 존재하는 컬럼만 필터링
    valid_subset = [
        col for col in subset_cols if col in df.columns and col in existing_df.columns
    ]

    if valid_subset:
        print(f"중복 제거 기준 컬럼: {valid_subset}")
        # 기존 데이터에 있는 키 조합 확인
        existing_keys = existing_df[valid_subset].apply(tuple, axis=1)
        # 새 데이터의 키 조합 확인
        new_keys = df[valid_subset].apply(tuple, axis=1)

        # 중복되지 않는 데이터만 필터링
        new_data = df[~new_keys.isin(existing_keys)]

        duplicate_count = len(df) - len(new_data)
        if duplicate_count > 0:
            print(f"중복 데이터 {duplicate_count}건 제외됨.")
    else:
        print("중복 제거를 위한 공통 컬럼이 부족하여 전체 중복 비교를 수행합니다.")
        common_cols = df.columns.intersection(existing_df.columns)
        is_duplicate = (
            df[common_cols]
            .apply(tuple, 1)
            .isin(existing_df[common_cols].apply(tuple, 1))
        )
        new_data = df[~is_duplicate]
else:
    new_data = df

# 새로운 event_id 삽입 과정
if not new_data.empty and "event_id" not in new_data.columns:
    new_data.insert(
        0, "event_id", range(last_event_id + 1, last_event_id + len(new_data) + 1)
    )

# 기존 데이터와 합치지 않고, 새로 포맷팅된 데이터만 저장
final_df = new_data

# 변환 완료 후 새로운 CSV 파일에 저장
output_file_path = f"./{new_data_name}_filled.csv"
try:
    final_df.to_csv(
        output_file_path,
        index=False,
        na_rep="NULL",
        encoding="utf-8-sig",
    )
    print(f"포맷팅된 데이터가 {output_file_path} 파일에 저장되었습니다.")

except Exception as e:
    print(f"파일 저장 실패: {e}")
