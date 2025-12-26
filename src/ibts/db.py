from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional
from datetime import datetime

import psycopg
from psycopg.rows import dict_row


@dataclass(frozen=True)
class DBCfg:
    host: str
    port: int
    database: str
    user: str
    password: str


def build_dsn(cfg: DBCfg) -> str:
    return (
        f"host={cfg.host} port={cfg.port} dbname={cfg.database} "
        f"user={cfg.user} password={cfg.password}"
    )


INSERT_SQL = """
INSERT INTO market.market_bars (
  symbol, timeframe, ts, open, high, low, close, volume, wap, bar_count, source
) VALUES (
  %(symbol)s, %(timeframe)s, %(ts)s, %(open)s, %(high)s, %(low)s, %(close)s,
  %(volume)s, %(wap)s, %(bar_count)s, %(source)s
)
ON CONFLICT (symbol, timeframe, ts) DO NOTHING;
"""

LAST_TS_SQL = """
SELECT max(ts) AS last_ts
FROM market.market_bars
WHERE symbol = %s AND timeframe = %s;
"""



class DB:
    def __init__(self, dsn: str):
        self._dsn = dsn
        self._conn: Optional[psycopg.Connection] = None

    def connect(self) -> None:
        self._conn = psycopg.connect(self._dsn, autocommit=True, row_factory=dict_row)

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def get_last_ts(self, symbol: str, timeframe: str) -> Optional[datetime]:
        assert self._conn
        with self._conn.cursor() as cur:
            cur.execute(LAST_TS_SQL, (symbol, timeframe))
            row = cur.fetchone()
            return row["last_ts"] if row else None

    def insert_bars(self, rows: Iterable[dict]) -> int:
        """
        Returns count of rows attempted. Duplicates will be ignored by ON CONFLICT.
        """
        assert self._conn
        rows = list(rows)
        if not rows:
            return 0

        with self._conn.cursor() as cur:
            cur.executemany(INSERT_SQL, rows)

        return len(rows)
