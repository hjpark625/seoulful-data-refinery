import argparse
import os
from pathlib import Path

import psycopg

from sync_postgres import load_env_file


DEFAULT_ENV_PATH = Path("./.env")
DEFAULT_MIGRATIONS_PATH = Path("./sql")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Railway PostgreSQL에 프로젝트 SQL 마이그레이션을 적용합니다."
    )
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_PATH)
    parser.add_argument("--migrations-dir", type=Path, default=DEFAULT_MIGRATIONS_PATH)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_env_file(args.env_file)
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL이 필요합니다.")
    if not args.migrations_dir.exists():
        raise FileNotFoundError(f"마이그레이션 디렉터리를 찾을 수 없습니다: {args.migrations_dir}")

    migration_files = sorted(args.migrations_dir.glob("*.sql"))
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    filename text PRIMARY KEY,
                    applied_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )

        for migration_path in migration_files:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT 1 FROM schema_migrations WHERE filename = %s",
                    (migration_path.name,),
                )
                if cursor.fetchone():
                    print(f"이미 적용됨: {migration_path.name}")
                    continue

                cursor.execute(migration_path.read_text(encoding="utf-8"))
                cursor.execute(
                    "INSERT INTO schema_migrations (filename) VALUES (%s)",
                    (migration_path.name,),
                )
            connection.commit()
            print(f"적용 완료: {migration_path.name}")


if __name__ == "__main__":
    main()
