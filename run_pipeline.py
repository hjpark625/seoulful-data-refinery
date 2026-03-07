import argparse
import subprocess
import sys
from pathlib import Path

import pandas as pd


DEFAULT_PREVIOUS_PATH = Path("./서울시 문화행사 정보(2.1).csv")
DEFAULT_CURRENT_PATH = Path("./서울시 문화행사 정보(3.7).csv")
DEFAULT_OUTPUT_PATH = Path("./서울시 문화행사 정보(3.7)_filled.csv")
DEFAULT_NEW_OUTPUT_PATH = Path("./서울시 문화행사 정보(3.7)_new.csv")
DEFAULT_ENV_PATH = Path("./.env")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="CSV 정제와 신규 데이터 Supabase 동기화를 순차 실행합니다."
    )
    parser.add_argument(
        "--previous",
        type=Path,
        default=DEFAULT_PREVIOUS_PATH,
        help="이전 스냅샷 원본 CSV 경로",
    )
    parser.add_argument(
        "--current",
        type=Path,
        default=DEFAULT_CURRENT_PATH,
        help="현재 스냅샷 원본 CSV 경로",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="현재 스냅샷 정제본 CSV 경로",
    )
    parser.add_argument(
        "--new-output",
        type=Path,
        default=DEFAULT_NEW_OUTPUT_PATH,
        help="신규 데이터만 담은 CSV 경로",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=DEFAULT_ENV_PATH,
        help="Supabase 환경변수를 읽을 .env 파일 경로",
    )
    parser.add_argument(
        "--table",
        default="events",
        help="동기화 대상 Supabase 테이블명",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Supabase insert 배치 크기",
    )
    parser.add_argument(
        "--skip-sync",
        action="store_true",
        help="CSV 생성까지만 수행하고 Supabase 동기화는 건너뜁니다.",
    )
    return parser.parse_args()


def run_command(command: list[str]) -> None:
    subprocess.run(command, check=True)


def count_new_rows(csv_path: Path) -> int:
    if not csv_path.exists():
        return 0

    new_rows_df = pd.read_csv(csv_path, encoding="utf-8-sig")
    return len(new_rows_df)


def main() -> None:
    args = parse_args()
    python_executable = sys.executable

    run_command(
        [
            python_executable,
            "main.py",
            "--previous",
            str(args.previous),
            "--current",
            str(args.current),
            "--output",
            str(args.output),
            "--new-output",
            str(args.new_output),
        ]
    )

    new_row_count = count_new_rows(args.new_output)
    print(f"신규 CSV 행 수: {new_row_count}건")

    if args.skip_sync:
        print("`--skip-sync` 옵션으로 Supabase 동기화를 건너뜁니다.")
        return

    if new_row_count == 0:
        print("신규 데이터가 없어 Supabase 동기화를 건너뜁니다.")
        return

    run_command(
        [
            python_executable,
            "sync_supabase.py",
            "--input",
            str(args.new_output),
            "--env-file",
            str(args.env_file),
            "--table",
            args.table,
            "--batch-size",
            str(args.batch_size),
        ]
    )


if __name__ == "__main__":
    main()
