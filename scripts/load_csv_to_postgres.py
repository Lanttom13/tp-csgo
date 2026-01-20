import os
import csv
from pathlib import Path
import psycopg2

RAW_DIR = Path("/data/raw")
FILES = {
    "results": RAW_DIR / "results.csv",
    "picks": RAW_DIR / "picks.csv",
    "economy": RAW_DIR / "economy.csv",
    "players": RAW_DIR / "players.csv",
}

def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

def read_header(csv_path: Path) -> list[str]:
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        return next(reader)

def main():
    db = os.environ["POSTGRES_DB"]
    user = os.environ["POSTGRES_USER"]
    pwd = os.environ["POSTGRES_PASSWORD"]
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = int(os.environ.get("POSTGRES_PORT", "5432"))

    for name, p in FILES.items():
        if not p.exists():
            raise FileNotFoundError(f"Missing {name}: {p}")

    conn = psycopg2.connect(dbname=db, user=user, password=pwd, host=host, port=port)
    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")

        for name, path in FILES.items():
            cols = read_header(path)
            table = f"staging.{name}"

            cur.execute(f"DROP TABLE IF EXISTS {table};")

            col_defs = ", ".join(f"{qident(c)} TEXT" for c in cols)
            cur.execute(f"CREATE TABLE {table} ({col_defs});")

            col_list = ", ".join(qident(c) for c in cols)
            copy_sql = f"COPY {table} ({col_list}) FROM STDIN WITH (FORMAT csv, HEADER true);"

            with path.open("r", encoding="utf-8") as f:
                cur.copy_expert(copy_sql, f)

            cur.execute(f"SELECT COUNT(*) FROM {table};")
            n = cur.fetchone()[0]
            print(f"Loaded {table}: {n} rows")

    conn.close()
    print("OK ✅ staging loaded")

if __name__ == "__main__":
    main()
