import argparse
import csv
import json
import os
import sys
from pathlib import Path
from urllib import parse, request

from sync_postgres import load_env_file


DEFAULT_ENV_PATH = Path("./.env")
DEFAULT_BATCH_SIZE = 1000
SOURCE_COLUMNS = (
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
)
TARGET_COLUMNS = SOURCE_COLUMNS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Supabase events를 PostgreSQL COPY SQL로 내보냅니다."
    )
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_PATH)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument(
        "--replace",
        action="store_true",
        help="대상 events 데이터를 같은 트랜잭션에서 비운 뒤 현재 Supabase 스냅샷으로 교체합니다.",
    )
    return parser.parse_args()


def fetch_rows(supabase_url: str, supabase_key: str, offset: int, limit: int) -> list[dict]:
    query = parse.urlencode(
        {
            "select": "*",
            "order": "event_id.asc",
            "offset": offset,
            "limit": limit,
        }
    )
    endpoint = f"{supabase_url.rstrip('/')}/rest/v1/events?{query}"
    req = request.Request(
        endpoint,
        headers={
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
        },
    )
    with request.urlopen(req, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def main() -> None:
    args = parse_args()
    if args.batch_size < 1:
        raise ValueError("batch_size는 1 이상이어야 합니다.")

    load_env_file(args.env_file)
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")
    if not supabase_url or not supabase_key:
        raise RuntimeError("SUPABASE_URL 및 SUPABASE_KEY가 필요합니다.")

    print("BEGIN;")
    if args.replace:
        print("TRUNCATE TABLE events;")
    print(
        "COPY events ("
        + ", ".join(TARGET_COLUMNS)
        + ") FROM STDIN WITH (FORMAT csv, NULL '');"
    )
    writer = csv.writer(sys.stdout, quoting=csv.QUOTE_NOTNULL, lineterminator="\n")

    offset = 0
    copied_count = 0
    while True:
        rows = fetch_rows(supabase_url, supabase_key, offset, args.batch_size)
        if not rows:
            break

        for row in rows:
            writer.writerow([row.get(column) for column in SOURCE_COLUMNS])
            copied_count += 1

        offset += len(rows)
        if len(rows) < args.batch_size:
            break

    print("\\.")
    print("COMMIT;")
    print(f"\\echo Supabase events {copied_count}건을 Railway PostgreSQL로 복사했습니다.")


if __name__ == "__main__":
    main()
