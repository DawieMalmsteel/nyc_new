#!/usr/bin/env python3
"""Execute every analytics question in sql/analytics_questions.sql and
assert each returns at least one row.

Exits non-zero if any question returns 0 rows.
"""
import re
import sys
import time
from pathlib import Path

from trino.dbapi import connect


SQL_PATH = Path(__file__).resolve().parent.parent / "sql" / "analytics_questions.sql"


def split_questions(sql: str) -> list[str]:
    # Drop the file header: everything from the top until the first "-- 1)" title.
    m = re.search(r"^--\s*1\)", sql, flags=re.MULTILINE)
    if m:
        sql = sql[m.start():]
    # Split on lines like "-- 1) ..."; keep only what comes after each title.
    chunks = re.split(r"^--\s*\d+\)\s.*?\n", sql, flags=re.MULTILINE)
    out = []
    for c in chunks:
        c = c.strip().rstrip(";\n ")
        if c:
            out.append(c)
    return out


def main() -> int:
    raw = SQL_PATH.read_text(encoding="utf-8")
    questions = split_questions(raw)
    print(f"[analytics] {len(questions)} questions found in {SQL_PATH.name}")

    conn = connect(host="localhost", port=8083, user="analytics")
    cur = conn.cursor()
    failures = []
    for i, q in enumerate(questions, 1):
        t0 = time.time()
        try:
            cur.execute(q)
            rows = cur.fetchall()
        except Exception as e:  # noqa: BLE001
            dt = time.time() - t0
            print(f"[Q{i}] ERROR ({dt:.2f}s): {e}")
            print("    " + q.splitlines()[0])
            failures.append((i, str(e)))
            continue
        n = len(rows)
        dt = time.time() - t0
        first = rows[0] if rows else None
        preview = str(first)[:80] if first else "(empty)"
        print(f"[Q{i}] {n:>4} rows in {dt:.2f}s | first: {preview}")
        if n == 0:
            failures.append((i, "zero rows"))
            print("    " + q.splitlines()[0])

    conn.close()
    if failures:
        print(f"\n[analytics] FAILED {len(failures)}/{len(questions)}")
        for i, err in failures:
            print(f"  Q{i}: {err}")
        return 1
    print(f"\n[analytics] PASS {len(questions)}/{len(questions)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
