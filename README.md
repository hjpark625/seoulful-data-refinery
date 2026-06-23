# 서울시 문화행사 데이터 정제 및 Railway PostgreSQL 적재

서울시 문화행사 CSV를 정제해 신규 행만 추출하고 Railway PostgreSQL의 `events` 테이블에 적재합니다.

## 구성

```plaintext
원본 CSV (이전/현재)
      ↓
main.py                 → 현재 정제 CSV + 신규 행 CSV
      ↓
sync_postgres.py        → Railway PostgreSQL events 적재

Supabase public 스키마 전체 이관
      ↓
migrate_supabase_database.py
```

`main.py`의 대화형 실행 흐름은 유지합니다. 적재 여부에 `y`를 입력하면 Supabase 대신 PostgreSQL로 신규 데이터를 넣습니다.

## 설치

```bash
pip install -r requirements.txt
```

## Railway PostgreSQL 설정

로컬에서 실행할 때는 Railway PostgreSQL의 **Public TCP Proxy** 연결 문자열을 `.env`에 넣습니다. 서버가 Railway 안에서 실행될 때는 Public URL이 아니라 Railway의 `DATABASE_URL` 참조 변수를 사용합니다.

```dotenv
DATABASE_URL=postgresql://...
```

`DATABASE_URL`과 비밀번호는 커밋하지 않습니다.
시작점으로 `.env.example`을 `.env`로 복사한 뒤 실제 연결 문자열을 채웁니다.

## 최초 스키마 생성

```bash
python apply_migrations.py
```

이 명령은 `sql/`의 마이그레이션을 한 번씩 적용합니다. `events` 테이블, 중복 방지 인덱스, API 조회 인덱스와 `category`·`gu` lookup 테이블/FK를 생성합니다.

## 일상 실행

```bash
python main.py
```

입력 예시:

```text
이전 스냅샷 일자를 입력하세요 (예: 3.7): 5.2
현재 스냅샷 일자를 입력하세요 (예: 4.8): 5.30

PostgreSQL에 신규 데이터를 적재하시겠습니까? [y/N]: y
```

`event_id`는 CSV 내부 비교용으로 계속 생성됩니다. DB 적재 시에는 Supabase와 동일한 `import_events_batch(jsonb)` 함수가 현재 최대 ID 다음 번호를 부여합니다.

단독 적재도 가능합니다.

```bash
python sync_postgres.py --input "서울시 문화행사 정보(5.30)_new.csv"
```

## 기존 Supabase 데이터 전체 이관

Supabase의 시스템 스키마(`auth`, `storage`)가 아니라 애플리케이션 테이블이 있는 `public` 스키마 전체를 이관합니다. Supabase Dashboard의 direct PostgreSQL connection string을 별도의 로컬 환경변수로 설정해야 합니다.

```dotenv
SUPABASE_DATABASE_URL=postgresql://postgres:...@db.<project-ref>.supabase.co:5432/postgres
DATABASE_URL=postgresql://...  # Railway Public TCP Proxy URL
```

PostgreSQL client 도구(`pg_dump`, `pg_restore`)가 설치된 상태에서 실행합니다. 현재처럼 `events`가 이미 REST 방식으로 복사된 상태에서 나머지 테이블만 이관하려면 `events`를 제외합니다.

```bash
python migrate_supabase_database.py --exclude-table events
```

대상이 비어 있지 않아 기존 동명 테이블을 교체해야 하는 경우에만 아래 옵션을 명시적으로 사용합니다.

```bash
python migrate_supabase_database.py --replace-target
```

## 파일 구성

```plaintext
main.py                            # CSV 정제와 대화형 PostgreSQL 적재
sync_postgres.py                   # 신규 CSV 배치 적재
apply_migrations.py                # SQL 마이그레이션 적용
migrate_supabase_database.py       # Supabase public 스키마 전체 덤프/복원
export_supabase_events_sql.py      # Supabase REST events를 COPY SQL로 변환
sql/001_events_postgres.sql        # events 스키마·인덱스
run_pipeline.py                    # 비대화형 정제·적재 실행
```
