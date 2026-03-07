import argparse
import json
import os
from pathlib import Path
from typing import Any
from urllib import error, parse, request

import pandas as pd


DEFAULT_INPUT_PATH = Path("./서울시 문화행사 정보(3.7)_new.csv")
DEFAULT_ENV_PATH = Path("./.env")
DEFAULT_TABLE = "events"
DEFAULT_BATCH_SIZE = 500


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="신규 CSV만 읽어 Supabase events 테이블에 적재합니다."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT_PATH,
        help="Supabase에 반영할 신규 데이터 CSV 경로",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=DEFAULT_ENV_PATH,
        help="Supabase 환경변수를 읽을 .env 파일 경로",
    )
    parser.add_argument(
        "--table",
        default=DEFAULT_TABLE,
        help="적재 대상 Supabase 테이블명",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="한 번에 insert 할 레코드 수",
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


def fetch_next_event_id(supabase_url: str, supabase_key: str, table: str) -> int:
    query = parse.urlencode(
        {
            "select": "event_id",
            "order": "event_id.desc",
            "limit": "1",
        }
    )
    endpoint = f"{supabase_url.rstrip('/')}/rest/v1/{table}?{query}"
    req = request.Request(
        endpoint,
        headers={
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
        },
        method="GET",
    )
    try:
        with request.urlopen(req) as response:
            rows = json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Supabase event_id 조회 실패 (status {exc.code}): {body}"
        ) from exc
    except error.URLError as exc:
        raise RuntimeError(f"Supabase 연결 실패: {exc}") from exc

    if not rows:
        return 1

    max_event_id = rows[0].get("event_id")
    return int(max_event_id) + 1 if max_event_id is not None else 1


def assign_event_ids(records: list[dict[str, Any]], start_event_id: int) -> list[dict[str, Any]]:
    assigned_records: list[dict[str, Any]] = []
    for offset, record in enumerate(records):
        assigned_record = record.copy()
        assigned_record["event_id"] = start_event_id + offset
        assigned_records.append(assigned_record)
    return assigned_records


def insert_into_supabase(
    records: list[dict[str, Any]],
    supabase_url: str,
    supabase_key: str,
    table: str,
    batch_size: int,
) -> None:
    if not records:
        print("신규 데이터가 없어 Supabase 쿼리를 실행하지 않습니다.")
        return

    endpoint = f"{supabase_url.rstrip('/')}/rest/v1/{table}"
    prepared_records = assign_event_ids(
        records,
        fetch_next_event_id(supabase_url, supabase_key, table),
    )

    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

    for start in range(0, len(prepared_records), batch_size):
        batch = prepared_records[start : start + batch_size]
        req = request.Request(
            endpoint,
            data=json.dumps(batch, ensure_ascii=False).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        try:
            with request.urlopen(req) as response:
                print(
                    "Supabase insert 완료: "
                    f"{start + 1}~{start + len(batch)} / {len(prepared_records)} "
                    f"(status {response.status})"
                )
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"Supabase insert 실패 (status {exc.code}): {body}"
            ) from exc
        except error.URLError as exc:
            raise RuntimeError(f"Supabase 연결 실패: {exc}") from exc


def main() -> None:
    args = parse_args()
    load_env_file(args.env_file)

    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        raise RuntimeError("SUPABASE_URL 또는 SUPABASE_SERVICE_ROLE_KEY가 필요합니다.")

    new_rows_df = read_new_rows(args.input)
    records = dataframe_to_records(new_rows_df)
    insert_into_supabase(
        records=records,
        supabase_url=supabase_url,
        supabase_key=supabase_key,
        table=args.table,
        batch_size=args.batch_size,
    )


if __name__ == "__main__":
    main()
