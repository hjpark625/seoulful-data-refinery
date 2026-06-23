import argparse
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

from sync_postgres import load_env_file


DEFAULT_ENV_PATH = Path("./.env")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Supabase PostgreSQL의 public 스키마 전체를 Railway PostgreSQL로 이관합니다."
    )
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_PATH)
    parser.add_argument(
        "--source-url-env",
        default="SUPABASE_DATABASE_URL",
        help="Supabase 직접 PostgreSQL 연결 문자열의 환경변수명",
    )
    parser.add_argument(
        "--schema",
        default="public",
        help="이관할 PostgreSQL 스키마명 (기본값: public)",
    )
    parser.add_argument(
        "--replace-target",
        action="store_true",
        help="대상 스키마의 동명 객체를 삭제한 뒤 복원합니다.",
    )
    parser.add_argument(
        "--exclude-table",
        action="append",
        default=[],
        help="public 스키마에서 제외할 테이블명입니다. 여러 번 지정할 수 있습니다.",
    )
    return parser.parse_args()


def require_command(name: str) -> None:
    if shutil.which(name) is None:
        raise RuntimeError(f"{name} 명령을 찾을 수 없습니다. PostgreSQL client 도구를 설치하세요.")


def main() -> None:
    args = parse_args()
    load_env_file(args.env_file)
    source_url = os.getenv(args.source_url_env)
    target_url = os.getenv("DATABASE_URL")
    if not source_url:
        raise RuntimeError(f"{args.source_url_env}이 필요합니다.")
    if not target_url:
        raise RuntimeError("DATABASE_URL이 필요합니다.")
    if not args.schema.replace("_", "").isalnum():
        raise ValueError("스키마명은 영문, 숫자, 밑줄만 사용할 수 있습니다.")
    for table in args.exclude_table:
        if not table.replace("_", "").isalnum():
            raise ValueError("제외할 테이블명은 영문, 숫자, 밑줄만 사용할 수 있습니다.")

    require_command("pg_dump")
    require_command("pg_restore")

    with tempfile.TemporaryDirectory(prefix="supabase-railway-") as temporary_directory:
        dump_path = Path(temporary_directory) / "supabase-public.dump"
        dump_command = [
            "pg_dump",
            f"--dbname={source_url}",
            "--format=custom",
            f"--file={dump_path}",
            f"--schema={args.schema}",
            "--no-owner",
            "--no-privileges",
        ]
        for table in args.exclude_table:
            dump_command.append(f"--exclude-table={args.schema}.{table}")
        subprocess.run(dump_command, check=True)

        restore_command = [
            "pg_restore",
            f"--dbname={target_url}",
            "--no-owner",
            "--no-privileges",
            "--exit-on-error",
        ]
        if args.replace_target:
            restore_command.extend(["--clean", "--if-exists"])
        restore_command.append(str(dump_path))
        subprocess.run(restore_command, check=True)

    print(f"Supabase {args.schema} 스키마를 Railway PostgreSQL로 이관했습니다.")


if __name__ == "__main__":
    main()
