import os
from pathlib import Path
from typing import Any

from sync_supabase import dataframe_to_records, insert_into_supabase, load_env_file

import pandas as pd

from enums.category import CategoryLabel, CategorySeq
from enums.gu import GuLabel, GuSeq
from utils.enum_mapping import get_enum_seq
from utils.geohash_calc import calculate_geohash


FILE_PREFIX = "서울시 문화행사 정보"

STANDARD_COLUMN_ORDER = [
    "event_id",
    "category_seq",
    "gu_seq",
    "event_name",
    "period",
    "place",
    "org_name",
    "use_target",
    "ticket_price",
    "inqury_number",
    "player",
    "describe",
    "etc_desc",
    "homepage_link",
    "main_img",
    "reg_date",
    "is_public",
    "start_date",
    "end_date",
    "theme",
    "latitude",
    "longitude",
    "is_free",
    "detail_url",
    "display_time",
    "geohash",
]

COLUMN_RENAME_MAP = {
    "Period": "period",
    "Player": "player",
}

OBJECT_COLUMNS = [
    "event_name",
    "period",
    "place",
    "org_name",
    "use_target",
    "ticket_price",
    "inqury_number",
    "player",
    "describe",
    "etc_desc",
    "homepage_link",
    "main_img",
    "reg_date",
    "start_date",
    "end_date",
    "theme",
    "detail_url",
    "display_time",
]

TEXTUAL_COMPARE_COLUMNS = [
    "event_name",
    "period",
    "place",
    "org_name",
    "use_target",
    "ticket_price",
    "inqury_number",
    "player",
    "describe",
    "etc_desc",
    "homepage_link",
    "main_img",
    "reg_date",
    "start_date",
    "end_date",
    "theme",
    "detail_url",
    "display_time",
]


def prompt_dates() -> tuple[str, str]:
    previous_date = input("이전 스냅샷 일자를 입력하세요 (예: 3.7): ").strip()
    current_date = input("현재 스냅샷 일자를 입력하세요 (예: 4.8): ").strip()
    return previous_date, current_date


def build_paths(previous_date: str, current_date: str) -> dict[str, Path]:
    return {
        "previous": Path(f"./{FILE_PREFIX}({previous_date}).csv"),
        "current": Path(f"./{FILE_PREFIX}({current_date}).csv"),
        "output": Path(f"./{FILE_PREFIX}({current_date})_filled.csv"),
        "new_output": Path(f"./{FILE_PREFIX}({current_date})_new.csv"),
    }


def load_csv_smartly(file_path: Path) -> pd.DataFrame:
    if not file_path.exists():
        print(f"파일이 없어 건너뜁니다: {file_path}")
        return pd.DataFrame()

    encodings = ["utf-8", "utf-8-sig", "cp949", "euc-kr", "latin-1"]

    for encoding in encodings:
        try:
            df = pd.read_csv(file_path, encoding=encoding)
            df = normalize_columns(df)

            if looks_like_mojibake(df):
                repaired_df = repair_mojibake(df.copy())
                repaired_df = normalize_columns(repaired_df)
                print(f"모지바케 복구 적용: {file_path} (로드 인코딩 {encoding})")
                return repaired_df

            print(f"CSV 로드 성공: {file_path} (인코딩 {encoding})")
            return df
        except UnicodeDecodeError:
            continue
        except Exception as exc:
            print(f"CSV 로드 실패: {file_path} (인코딩 {encoding}) - {exc}")

    raise ValueError(f"지원된 인코딩으로 파일을 읽지 못했습니다: {file_path}")


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns=COLUMN_RENAME_MAP)


def looks_like_mojibake(df: pd.DataFrame) -> bool:
    for column in ["category_seq", "gu_seq", "event_name"]:
        if column not in df.columns:
            continue

        sample = df[column].dropna().astype(str).head(20).tolist()
        if not sample:
            continue

        suspicious_count = sum(
            any(char in value for char in ["À", "Ã", "½", "°", "¿", "¼", "¾"])
            for value in sample
        )
        if suspicious_count >= max(1, len(sample) // 2):
            return True

    return False


def repair_mojibake(df: pd.DataFrame) -> pd.DataFrame:
    def fix_text(value: Any) -> Any:
        if not isinstance(value, str):
            return value
        try:
            return value.encode("latin-1").decode("cp949")
        except (UnicodeEncodeError, UnicodeDecodeError):
            return value

    for column in df.select_dtypes(include=["object"]).columns:
        df[column] = df[column].apply(fix_text)

    return df


def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=STANDARD_COLUMN_ORDER)

    df = normalize_columns(df.copy())

    for column in STANDARD_COLUMN_ORDER:
        if column not in df.columns and column != "event_id" and column != "geohash":
            df[column] = pd.NA

    df = df.dropna(how="all").copy()
    df = validate_and_convert_lat_lon(df)
    df["geohash"] = df.apply(safe_calculate_geohash, axis=1)

    category_label_to_seq = {
        label.value: seq.value for label, seq in zip(CategoryLabel, CategorySeq)
    }
    gu_label_to_seq = {label.value: seq.value for label, seq in zip(GuLabel, GuSeq)}

    df["category_seq"] = df["category_seq"].apply(
        lambda value: get_enum_seq(
            normalize_text(value),
            category_label_to_seq,
            CategorySeq.OTHER.value,
        )
    )
    df["gu_seq"] = df["gu_seq"].apply(
        lambda value: get_enum_seq(
            normalize_text(value),
            gu_label_to_seq,
            GuSeq.OTHER.value,
        )
    )

    df["is_public"] = df["is_public"].apply(parse_public_flag)
    df["is_free"] = df["is_free"].apply(parse_free_flag)

    for column in OBJECT_COLUMNS:
        df[column] = df[column].apply(normalize_text)

    df["latitude"] = df["latitude"].astype(float)
    df["longitude"] = df["longitude"].astype(float)

    # 현재 스냅샷의 event_id는 파일 내부 순번으로 재생성합니다.
    df = df.reset_index(drop=True)
    df.insert(0, "event_id", range(1, len(df) + 1))

    df = df[STANDARD_COLUMN_ORDER].copy()
    df = df.drop_duplicates(subset=["detail_url", "event_name", "start_date", "place"])

    return df.reset_index(drop=True)


def normalize_text(value: Any) -> str:
    if pd.isna(value):
        return "NULL"

    normalized = str(value).strip()
    return normalized if normalized else "NULL"


def validate_and_convert_lat_lon(
    df: pd.DataFrame,
    lat_column: str = "latitude",
    lon_column: str = "longitude",
) -> pd.DataFrame:
    def convert(value: Any) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    df[lat_column] = df[lat_column].apply(convert)
    df[lon_column] = df[lon_column].apply(convert)

    swap_condition = (df[lat_column] > 90) & (df[lon_column] < 90)
    if swap_condition.any():
        print(f"위도/경도 뒤바뀜 감지: {swap_condition.sum()}건 수정")
        df.loc[swap_condition, [lat_column, lon_column]] = df.loc[
            swap_condition, [lon_column, lat_column]
        ].values

    before_count = len(df)
    df = df.dropna(subset=[lat_column, lon_column]).copy()
    dropped_count = before_count - len(df)
    if dropped_count:
        print(f"좌표 누락으로 제외된 행: {dropped_count}건")

    return df


def safe_calculate_geohash(row: pd.Series) -> str:
    try:
        return calculate_geohash(row)
    except Exception:
        return "NULL"


def parse_public_flag(value: Any) -> bool:
    normalized = normalize_text(value)
    return normalized == "기관"


def parse_free_flag(value: Any) -> bool:
    normalized = normalize_text(value)
    return normalized == "무료"


def build_comparison_key(row: pd.Series) -> str:
    detail_url = normalize_text(row.get("detail_url"))
    if detail_url != "NULL":
        return f"detail_url::{detail_url}"

    fields = [
        normalize_text(row.get("event_name")),
        normalize_text(row.get("start_date")),
        normalize_text(row.get("place")),
        normalize_text(row.get("geohash")),
    ]
    return "fallback::" + "||".join(fields)


def has_replacement_char(value: Any) -> bool:
    return isinstance(value, str) and "\ufffd" in value


def restore_from_previous_snapshot(
    previous_df: pd.DataFrame,
    current_df: pd.DataFrame,
) -> pd.DataFrame:
    if previous_df.empty or current_df.empty:
        return current_df

    restored_df = current_df.copy()
    previous_indexed = previous_df.copy()
    previous_indexed["_comparison_key"] = previous_indexed.apply(
        build_comparison_key, axis=1
    )
    previous_lookup = previous_indexed.set_index("_comparison_key")

    restored_count = 0
    for index, row in restored_df.iterrows():
        comparison_key = build_comparison_key(row)
        if comparison_key not in previous_lookup.index:
            continue

        previous_row = previous_lookup.loc[comparison_key]
        for column in STANDARD_COLUMN_ORDER:
            if column == "event_id":
                continue
            current_value = row[column]
            if has_replacement_char(current_value):
                restored_df.at[index, column] = previous_row[column]
                restored_count += 1

    if restored_count:
        print(f"이전 스냅샷 기준 텍스트 복원: {restored_count}개 필드")

    return restored_df


def count_rows_with_broken_text(df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    mask = pd.Series(False, index=df.index)
    for column in TEXTUAL_COMPARE_COLUMNS:
        if column not in df.columns:
            continue
        mask = mask | df[column].apply(has_replacement_char)
    return int(mask.sum())


def detect_new_rows(
    previous_df: pd.DataFrame,
    current_df: pd.DataFrame,
) -> pd.DataFrame:
    if current_df.empty:
        return current_df.copy()

    current_keys = current_df.apply(build_comparison_key, axis=1)
    if previous_df.empty:
        print("이전 스냅샷이 없어 현재 전체 데이터를 신규로 간주합니다.")
        return current_df.assign(_comparison_key=current_keys).drop(
            columns=["_comparison_key"], errors="ignore"
        )

    previous_keys = set(previous_df.apply(build_comparison_key, axis=1).tolist())
    new_rows = current_df.loc[~current_keys.isin(previous_keys)].copy()
    print(f"신규 데이터 감지: {len(new_rows)}건 / 현재 전체 {len(current_df)}건")
    return new_rows


def save_csv(df: pd.DataFrame, output_path: Path) -> None:
    df.to_csv(output_path, index=False, na_rep="NULL", encoding="utf-8-sig")
    print(f"CSV 저장 완료: {output_path} ({len(df)}건)")


def main() -> None:
    previous_date, current_date = prompt_dates()
    paths = build_paths(previous_date, current_date)

    previous_raw_df = load_csv_smartly(paths["previous"])
    current_raw_df = load_csv_smartly(paths["current"])

    previous_clean_df = prepare_dataframe(previous_raw_df)
    current_clean_df = prepare_dataframe(current_raw_df)
    current_clean_df = restore_from_previous_snapshot(
        previous_clean_df, current_clean_df
    )
    new_rows_df = detect_new_rows(previous_clean_df, current_clean_df)

    save_csv(current_clean_df, paths["output"])
    save_csv(new_rows_df, paths["new_output"])

    broken_current_rows = count_rows_with_broken_text(current_clean_df)
    broken_new_rows = count_rows_with_broken_text(new_rows_df)
    if broken_current_rows:
        print(
            "경고: 현재 정제본에 원본부터 손상된 문자열이 남아 있습니다. "
            f"{broken_current_rows}개 행"
        )
    if broken_new_rows:
        print(
            "경고: 신규 데이터 중 원본부터 손상된 문자열이 남아 있습니다. "
            f"{broken_new_rows}개 행"
        )

    answer = input("\nSupabase에 신규 데이터를 적재하시겠습니까? [y/N]: ").strip().lower()
    if answer != "y":
        print("Supabase 적재를 건너뜁니다.")
        return

    load_env_file(Path("./.env"))

    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        raise RuntimeError("SUPABASE_URL 또는 SUPABASE_SERVICE_ROLE_KEY가 필요합니다.")

    records = dataframe_to_records(new_rows_df)
    insert_into_supabase(
        records=records,
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        table="events",
        batch_size=500,
    )


if __name__ == "__main__":
    main()
