import argparse
import json
import os
import re
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg


DEFAULT_INPUT_PATH = Path("./서울시 문화행사 정보(3.7)_new.csv")
DEFAULT_ENV_PATH = Path("./.env")
DEFAULT_TABLE = "events"
DEFAULT_BATCH_SIZE = 500

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="신규 CSV를 Railway PostgreSQL events 테이블에 적재합니다."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT_PATH,
        help="PostgreSQL에 반영할 신규 데이터 CSV 경로",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=DEFAULT_ENV_PATH,
        help="DATABASE_URL을 읽을 .env 파일 경로",
    )
    parser.add_argument(
        "--table",
        default=DEFAULT_TABLE,
        help="적재 대상 PostgreSQL 테이블명",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="한 트랜잭션에서 insert 할 레코드 수",
    )
    return parser.parse_args()


def load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue

        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'").strip('"'))


def read_new_rows(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"신규 CSV 파일을 찾을 수 없습니다: {csv_path}")

    return pd.read_csv(csv_path, encoding="utf-8-sig")


def dataframe_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []

    payload_df = df.copy().where(pd.notna(df), None)
    records: list[dict[str, Any]] = []
    for row in payload_df.to_dict(orient="records"):
        serialized_row: dict[str, Any] = {}
        for key, value in row.items():
            if value == "NULL":
                serialized_row[key] = None
            elif isinstance(value, pd.Timestamp):
                serialized_row[key] = value.isoformat()
            else:
                serialized_row[key] = value
        records.append(serialized_row)
    return records


def validate_table_name(table: str) -> None:
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", table):
        raise ValueError("테이블명은 소문자, 숫자, 밑줄만 사용할 수 있습니다.")


def insert_into_postgres(
    records: list[dict[str, Any]],
    database_url: str,
    table: str,
    batch_size: int,
) -> None:
    if not records:
        print("신규 데이터가 없어 PostgreSQL 쿼리를 실행하지 않습니다.")
        return
    if batch_size < 1:
        raise ValueError("batch_size는 1 이상이어야 합니다.")

    validate_table_name(table)
    if table != "events":
        raise ValueError("현재 적재 함수는 events 테이블만 지원합니다.")

    with psycopg.connect(database_url) as connection:
        for start in range(0, len(records), batch_size):
            batch = records[start : start + batch_size]
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT public.import_events_batch(%s::jsonb)",
                    (json.dumps(batch, ensure_ascii=False, default=str),),
                )
                inserted_count = cursor.fetchone()[0]
            connection.commit()
            print(
                "PostgreSQL insert 완료: "
                f"{start + 1}~{start + len(batch)} / {len(records)} "
                f"(신규 {inserted_count}건)"
            )


def main() -> None:
    args = parse_args()
    load_env_file(args.env_file)

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL이 필요합니다.")

    new_rows_df = read_new_rows(args.input)
    records = dataframe_to_records(new_rows_df)
    insert_into_postgres(
        records=records,
        database_url=database_url,
        table=args.table,
        batch_size=args.batch_size,
    )


if __name__ == "__main__":
    main()
